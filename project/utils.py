import logging
import os
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql.functions import to_json, struct, col, from_json, current_date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def create_logger() -> logging.Logger:
    """
    Создает логгер, устанавливает уровень логгирования и формирует формат вывода

    :return: логгер
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def producer_to_kafka(data: DataFrame, topic: str, host: str, port: int, logger: logging.Logger) -> str:
    """
    Отправляет данные в Kafka

    :param data: данные
    :param topic: название топика
    :param host: хост Kafka
    :param port: порт Kafka
    :param logger: логгер
    :return: сообщение
    """
    kafka_options = {
        "kafka.bootstrap.servers": f"{host}:{port}",
        "topic": topic
    }
    data = data.cache()
    num_rows = data.count()
    if num_rows == 0:
        return f'df is empty'
    try:
        (data
         .select(to_json(struct(*[col(c) for c in data.columns])).alias("value"))
         .write
         .format("kafka")
         .options(**kafka_options)
         .save()
         )
        data.unpersist()
        return f'{num_rows} rows were send to {kafka_options["topic"]} successfully at {datetime.now()}'
    except Exception as e:
        logger.error(e)
    finally:
        data.unpersist()


def write_to_psql(df: DataFrame, table_name: str, columns: list) -> str:
    """
    Запись данных в PostgreSQL

    :param columns:
    :param table_name: название таблицы
    :param df: данные о квартирах
    :return: message: сообщение о выполнении
    """
    host = os.getenv("HOST", 'localhost')
    port = os.getenv("PORT", 5435)
    database = os.getenv("DB", 'docker_app_db')
    user = os.getenv("DB_USERNAME", 'docker_app')
    password = os.getenv("POSTGRES_PASSWORD", 'docker_app')
    num_rows = df.count()

    df = df.select(*columns).withColumn('created_at', current_date())
    (df
     .write
     .format("jdbc")
     .mode("append")
     .option("dbtable", table_name)
     .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
     .option("user", user)
     .option("password", password)
     .option("driver", "org.postgresql.Driver")
     .save())

    message = f'{num_rows} rows written to {table_name} successfully at {datetime.now()}'
    return message


def kafka_consumer(spark_session: SparkSession,
                   host: str,
                   port: int,
                   topic: str,
                   schema: StructType,
                   columns: list,
                   process_batch: callable) -> None:
    """
    Функция принимает данные из Kafka и обрабатывает их в функции обработки пакета

    :param spark_session: спарк-сессия
    :param host: хост Kafka
    :param port: порт Kafka
    :param topic: топик Kafka
    :param schema: схема данных
    :param columns: колонки df
    :param process_batch: функция обработки пакета
    :return: None
    """

    kafka_options = {
        "kafka.bootstrap.servers": f"{host}:{port}",
        "topic": topic
    }

    df = (spark_session.readStream
          .format("kafka")
          .option("subscribe", topic)
          .options(**kafka_options)
          .load()
          )

    processed_df = df.select(
        col("key"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("key", "data.*").select(*columns)

    query = processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()


def get_data_psql(spark_session: SparkSession, table_name: str) -> DataFrame:
    """
    Функция принимает данные из PostgreSQL и обрабатывает их

    :param spark_session:
    :param table_name:
    :return:
    """
    host = os.getenv("HOST", 'localhost')
    port = os.getenv("PORT", 5435)
    database = os.getenv("DB", 'docker_app_db')
    user = os.getenv("DB_USERNAME", 'docker_app')
    password = os.getenv("POSTGRES_PASSWORD", 'docker_app')

    pg_df = (spark_session.read
             .format("jdbc")
             .option("dbtable", table_name)
             .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
             .option("user", user)
             .option("password", password)
             .option("driver", "org.postgresql.Driver")
             .load()
             )
    if pg_df.take(1):
        return pg_df


def spark_configs() -> SparkConf:
    """
    Функция возвращает SparkConf с конфигурационными настройками Spark

    :return: SparkConf
    """
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.postgresql:postgresql:42.2.24"
        ]
    )
    """Функция возвращает объект SparkConf с конфигурационными настройками Spark"""
    default_configs = [("spark.sql.shuffle.partitions", '500'),
                       ("spark.sql.session.timeZone", "UTC"),
                       ("spark.jars.packages", spark_jars_packages),
                       ]
    return SparkConf().setAll(default_configs)


def create_spark_session(app_name: str) -> SparkSession:
    """Функция создает SparkSession

    :param app_name: название приложения
    :return: SparkSession
    """
    spark = (SparkSession.builder
             .appName(app_name)
             .config(conf=spark_configs())
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel('WARN')
    return spark
