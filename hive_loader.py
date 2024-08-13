"""
Модуль обеспечивает наполнение Hive таблиц данными из Kafka

"""
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date


def write_to_hive(df: DataFrame, epoch_id) -> None:
    result = df.withColumn('created_at', current_date())
    (result.write
     .format("parquet")
     .mode("append")
     .partitionBy('created_at')
     .saveAsTable(os.getenv("FLATS_HIVE_TABLE"))
     )


def kafka_consumer():
    spark = SparkSession.builder.getOrCreate()

    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS"))  # your_kafka_broker_host:port
          .option("subscribe", "flats_data")
          .load()
          )

    processed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    query = processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_hive) \
        .start()

    query.awaitTermination()


kafka_consumer()
