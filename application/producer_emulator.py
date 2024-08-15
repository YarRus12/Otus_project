import os
from typing import Generator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id, to_json, struct, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
import random
import requests

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    ]
)


def get_streets_names():
    url = "https://my.api.mockaroo.com/otus_api.json"
    headers = {
        "X-API-Key": "045554c0",  # это следовало бы поместить в переменную окружения
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.text.split("\n")
        streets = []
        for row in data[1:]:
            streets.append(row.strip().split(",")[0])
        return streets


def get_cities_names(spark_session: SparkSession):
    cities = [row[0] for row in (spark_session.read.format('csv').option('header', 'true')
                                 .load('/Users/iaroslavrussu/Dropbox/OTUS_project/city.csv')
                                 .select('city')
                                 ).collect()]
    return cities


def generate_random_data(record_num, cities: list, streets: list) -> Generator:
    """
        Создаем случайные данные о квартирах
    """
    for i in range(record_num):
        yield {
            "city": random.choice(cities),
            "street": random.choice(streets),
            "floor": random.randint(1, 20),
            "rooms": random.randint(1, 5),
            "price": random.randint(1000000, 10000000)
        }


def create_data(spark_session: SparkSession, cities: list, streets: list, record_num: int):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True),
        StructField("price", IntegerType(), True)
    ])
    data = generate_random_data(record_num=record_num, cities=cities, streets=streets)
    dataframe = spark_session.createDataFrame(data, schema)
    return dataframe.withColumn("id", monotonically_increasing_id())


def producer_to_kafka(data: DataFrame, topic: str):
    kafka_options = {
        "kafka.bootstrap.servers": "localhost:9092",
        "topic": topic
    }
    (data
     .select(to_json(struct(*[col(c) for c in data.columns])).alias("value"))
     .write
     .format("kafka")
     .options(**kafka_options)
     .save()
     )


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    streets_names = get_streets_names()
    cities_names = get_cities_names(spark_session=spark)
    record_num = 500
    topic = "new_data"
    generated_df = (create_data(spark, cities=cities_names, streets=streets_names, record_num=record_num)
                    )
    producer_to_kafka(data=generated_df, topic=topic)
