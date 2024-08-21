import threading
import time
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from utils import producer_to_kafka, create_logger, create_spark_session, kafka_consumer, write_to_psql
from flask import Flask


def execute_model(path, dataframe):
    loaded_model = PipelineModel.load(path)

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


def process_batch(dataframe: DataFrame, epoch_id) -> None:
    """
    Обработка данных в пакете

    :param dataframe: данные в пакете
    :param epoch_id:
    :return: None
    """
    result = execute_model(path="models", dataframe=dataframe).cache()
    message = producer_to_kafka(data=result, topic="answers", host="localhost", port=9092, logger=logger)
    logger.info(message)

    columns = ["id", "prediction"]
    message = write_to_psql(df=result, table_name='STAGE.ANSWERS_TABLE', columns=columns)
    logger.info(message)
    result.unpersist()


app = Flask(__name__)


def start_kafka_consumer():
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True)
    ])
    kafka_consumer(spark_session=spark, host="localhost", port=9092, topic="requests",
                   schema=schema, process_batch=process_batch, columns=["id", "city", "street", "floor", "rooms"])


def run_kafka_consumer():
    while True:
        start_kafka_consumer()
        time.sleep(60)


@app.route('/predict', methods=['POST'])
def predict_route():
    return True


if __name__ == "__main__":
    logger = create_logger()
    spark = create_spark_session(app_name="ModelApplication")
    thread = threading.Thread(target=run_kafka_consumer)
    thread.start()
    model_path = "../models"
    app.run(debug=True, port=5001)
