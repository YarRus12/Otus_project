import os
from functools import partial
from apscheduler.schedulers.background import BackgroundScheduler
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from flask import Flask, jsonify

# Обработка ошибки при запуске в docker
try:
    from utils import producer_to_kafka, create_logger, create_spark_session, kafka_consumer, write_to_psql
except ModuleNotFoundError:
    from .utils import producer_to_kafka, create_logger, create_spark_session, kafka_consumer, write_to_psql



def load_model(path: str) -> PipelineModel:
    """
    Загрузка модели

    :param path: путь к модели
    :return: модель
    """
    return PipelineModel.load(path)


def execute_model(model:PipelineModel, dataframe: DataFrame) -> DataFrame:
    """
    Применение модели к данным

    :param model: применяемая модель
    :param dataframe: данные из пакета
    :return: расчитанные значения предсказаний
    """
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
    result = model.transform(final_df)
    return result.select("id", "prediction")


def process_batch(dataframe: DataFrame, epoch_id, model: PipelineModel) -> None:
    """
    Обработка данных в пакете

    :param dataframe: данные в пакете
    :param model: модель
    :param epoch_id:
    :return: None
    """
    result = execute_model(dataframe=dataframe, model=model).cache()
    message = producer_to_kafka(data=result, topic="answers", host=os.getenv('KAFKA_HOST', 'localhost'),
                                port=os.getenv('KAFKA_PORT', 9092), logger=logger)
    logger.info(message)

    columns = ["id", "prediction"]
    message = write_to_psql(df=result, table_name='STAGE.ANSWERS_TABLE', columns=columns)
    logger.info(message)
    result.unpersist()


app = Flask(__name__)


def start_kafka_consumer(spark_session: SparkSession) -> None:
    """
    Запуск обработчика Kafka

    :param spark_session: спарк-сессия
    :return: None
    """
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True)
    ])
    kafka_consumer(spark_session=spark_session, host=os.getenv('KAFKA_HOST', 'localhost'),
                                port=os.getenv('KAFKA_PORT', 9092), topic="requests",
                   schema=schema, process_batch=process_batch_partial,
                   columns=["id", "city", "street", "floor", "rooms"])


@app.route('/status', methods=['GET'])
def status() -> jsonify:
    """
    Вспомогательная функция для проверки работоспособности сервиса в логах контейнера

    :return: jsonify: Сообщение о состоянии сервиса
    """
    return jsonify({'message': 'Service is running'})


if __name__ == "__main__":
        logger = create_logger()
        spark = create_spark_session(app_name="DataLoader")
        scheduler = BackgroundScheduler()
        loaded_model = load_model(path="models")
        process_batch_partial = partial(process_batch, model=loaded_model)
        start_kafka_consumer_partial = partial(start_kafka_consumer, spark_session=spark)
        scheduler.add_job(func=start_kafka_consumer_partial, trigger="interval", seconds=60)
        scheduler.start()

        app.run(debug=True, host='0.0.0.0', use_reloader=True, port=8083)
