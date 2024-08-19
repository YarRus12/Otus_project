import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date, from_json, col, struct
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DateType

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.2.24"
    ]
)


def write_to_psql(df: DataFrame, epoch_id) -> None:
    host = 'localhost'
    port = 5435
    database = 'docker_app_db'
    df.show(truncate=False)
    (df
     .select("city", "street", "floor", "rooms", "price", current_date().alias("created_at"))
     .write
     .format("jdbc")
     .mode("append")
     .option("dbtable", "STAGE.FLATS_TABLE")
     .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
     .option("user", "docker_app")
     .option("password", "docker_app")
     .option("driver", "org.postgresql.Driver")
     .save())

    print('Data loaded successfully')


def process_batch(df: DataFrame, epoch_id) -> None:
    # Выводим данные на экран
    if df.count() > 0:
        write_to_psql(df, epoch_id)


def kafka_consumer(spark, topic="new_data"):
    kafka_options = {
        "kafka.bootstrap.servers": "localhost:9092",
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

    df = (spark.readStream
          .format("kafka")
          .option("subscribe", "new_data")
          .options(**kafka_options)
          .load()
          )

    processed_df = df.select(
        col("key"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("key", "data.*")

    # Распаковка структуры
    processed_df = processed_df.select("city", "street", "floor", "rooms", "price")

    query = processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    spark = (SparkSession.builder
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", spark_jars_packages)
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel('WARN')
    kafka_consumer(spark)
