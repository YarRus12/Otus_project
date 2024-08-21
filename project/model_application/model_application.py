import threading
import time
from functools import partial

from apscheduler.schedulers.background import BackgroundScheduler
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from project.utils import producer_to_kafka, create_logger, create_spark_session, kafka_consumer, write_to_psql
from flask import Flask, jsonify


def load_model(path) -> PipelineModel:
    return PipelineModel.load(path)


def execute_model(loaded_model:PipelineModel, dataframe: DataFrame) -> DataFrame:

    # Подтверждение успешной загрузки
    logger.info("Model loaded successfully")
    # Apply the same transformations as during training
    city_indexer = StringIndexer(inputCol='city', outputCol='city_index')
    city_indexer.setHandleInvalid("skip")
    street_indexer = StringIndexer(inputCol='street', outputCol='street_index')
    street_indexer.setHandleInvalid("skip")

    # Fit the indexers on the new data (this is necessary to create the index mapping)
    city_indexer_model = city_indexer.fit(dataframe)
    street_indexer_model = street_indexer.fit(dataframe)

    # Transform the data
    indexed_df = city_indexer_model.transform(dataframe)
    indexed_df = street_indexer_model.transform(indexed_df)

    # Assemble features
    assembler = VectorAssembler(inputCols=["city_index", "street_index", "floor", "rooms"], outputCol="features")
    final_df = assembler.transform(indexed_df)

    # Now you can use the loaded model to make predictions
    result = loaded_model.transform(final_df)
    return result.select("id", "prediction")


def process_batch(dataframe: DataFrame, epoch_id, loaded_model: PipelineModel) -> None:
    """
    Обработка данных в пакете

    :param dataframe: данные в пакете
    :param loaded_model: модель
    :param epoch_id:
    :return: None
    """
    result = execute_model(dataframe=dataframe, loaded_model=loaded_model).cache()
    message = producer_to_kafka(data=result, topic="answers", host="localhost", port=9092, logger=logger)
    logger.info(message)

    columns = ["id", "prediction"]
    message = write_to_psql(df=result, table_name='STAGE.ANSWERS_TABLE', columns=columns)
    logger.info(message)
    result.unpersist()


app = Flask(__name__)


def start_kafka_consumer(spark_session: SparkSession, loaded_model: PipelineModel):
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True)
    ])
    kafka_consumer(spark_session=spark_session, host="localhost", port=9092, topic="requests",
                   schema=schema, process_batch=process_batch_partial,
                   columns=["id", "city", "street", "floor", "rooms"])


@app.route('/status', methods=['GET'])
def status():
    return jsonify({'message': 'Service is running'})


if __name__ == "__main__":
        logger = create_logger()
        spark = create_spark_session(app_name="DataLoader")
        scheduler = BackgroundScheduler()
        loaded_model = load_model(path="models")
        process_batch_partial = partial(process_batch, loaded_model=loaded_model)
        start_kafka_consumer_partial = partial(start_kafka_consumer, spark_session=spark, loaded_model=loaded_model)
        scheduler.add_job(func=start_kafka_consumer_partial, trigger="interval", seconds=60)
        scheduler.start()

        app.run(debug=True, host='0.0.0.0', use_reloader=True, port=8083)

