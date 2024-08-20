import os
import time

import schedule
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date, from_json, col, struct
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DateType
import logging
from flask import Flask, jsonify

logging.basicConfig(level=logging.DEBUG)

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.2.24"
    ]
)


def write_to_psql(df: DataFrame, epoch_id) -> str:
    """
    Запись данных в PostgreSQL

    :param df: данные о квартирах
    :param epoch_id:
    :return: message: сообщение о выполнении
    """
    host = 'localhost'
    port = 5435
    database = 'docker_app_db'
    table_name = "STAGE.FLATS_TABLE"
    num_rows = df.count()

    (df
     .select("city", "street", "floor", "rooms", "price", current_date().alias("created_at"))
     .write
     .format("jdbc")
     .mode("append")
     .option("dbtable", table_name)
     .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
     .option("user", "docker_app")
     .option("password", "docker_app")
     .option("driver", "org.postgresql.Driver")
     .save())
    message = f'{num_rows} rows were send to {table_name} successfully at {datetime.now()}'
    return message


def process_batch(df: DataFrame, epoch_id) -> None:
    """
    Обработка данных в пакете

    :param df: данные в пакете
    :param epoch_id:
    :return: None
    """
    if df.take(1):
        message = write_to_psql(df.cache(), epoch_id)
        df.unpersist()
        print(message)


def kafka_consumer(spark_session: SparkSession, host: str, port: int, topic: str) -> None:
    kafka_options = {
        "kafka.bootstrap.servers": f"{host}:{port}",
        "topic": topic
    }

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True),
        StructField("price", LongType(), True)
    ])

    df = (spark_session.readStream
          .format("kafka")
          .option("subscribe", "new_data")
          .options(**kafka_options)
          .load()
          )

    processed_df = df.select(
        col("key"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("key", "data.*")

    processed_df = processed_df.select("city", "street", "floor", "rooms", "price")

    query = processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()


app = Flask(__name__)


def start_kafka_consumer():
    kafka_consumer(spark_session=spark, host="localhost", port=9092, topic="new_data")


schedule.every(2).minutes.do(start_kafka_consumer)  # запуск раз в 2 минуты

if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    spark = (SparkSession.builder
             .appName("DataLoader")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", spark_jars_packages)
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel('WARN')
    app.run(debug=True)

    while True:
        schedule.run_pending()
        time.sleep(10)
