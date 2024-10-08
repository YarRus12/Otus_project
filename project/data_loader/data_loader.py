import os
from functools import partial
from apscheduler.schedulers.background import BackgroundScheduler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from flask import Flask, jsonify

# Обработка ошибки при запуске в docker
try:
    from utils import write_to_psql, create_logger, create_spark_session, kafka_consumer
except ModuleNotFoundError:
    from .utils import write_to_psql, create_logger, create_spark_session, kafka_consumer

def process_batch(df: DataFrame, epoch_id) -> None:
    """
    Обработка данных в пакете

    :param df: данные в пакете
    :param epoch_id:
    :return: None
    """
    columns = ["city", "street", "floor", "rooms", "price"]
    message = write_to_psql(df=df.cache(), table_name="STAGE.FLATS_TABLE", columns=columns)
    logger.info(message)
    df.unpersist()


app = Flask(__name__)


@app.route('/start_kafka_consumer', methods=['GET'])
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
        StructField("rooms", IntegerType(), True),
        StructField("price", LongType(), True)
    ])
    kafka_consumer(spark_session=spark_session, host=os.getenv('KAFKA_HOST', 'localhost'),
                   port=os.getenv('KAFKA_PORT', 9092), topic="new_data", schema=schema,
                   columns=["id", "city", "street", "floor", "rooms", "price"],
                   process_batch=process_batch)

@app.route('/status', methods=['GET'])
def status() -> jsonify:
    """
    Вспомогательная функция для проверки работоспособности сервиса в логах контейнера

    :return: jsonify: Сообщение о состоянии сервиса
    """
    return jsonify({'message': 'Service is running'})


if __name__ == "__main__":
        logger = create_logger()
        logger.info("Start data loader")
        spark = create_spark_session(app_name="DataLoader")
        scheduler = BackgroundScheduler()
        start_kafka_consumer_partial = partial(start_kafka_consumer, spark_session=spark)
        scheduler.add_job(func=start_kafka_consumer_partial, trigger="interval", seconds=60)
        scheduler.start()

        app.run(debug=True, host='0.0.0.0', use_reloader=True, port=8085)
