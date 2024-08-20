import time
from typing import Generator
import schedule
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
import random
import requests
from flask import Flask, jsonify
from application.utils import producer_to_kafka, create_logger, create_spark_session


def get_streets_names() -> list:
    """
    Загружаем список улиц из mockaroo

    :return: streets : список улиц
    """
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


def get_streets_names_archives(spark_session: SparkSession) -> list:
    """
    Загружаем список улиц из csv если may.api.mockaroo.com недоступен

    :return: список улиц
    """
    data = [row[0] for row in (spark_session.read.format('csv').option('header', 'true')
                                 .load('./data/streets.csv')
                                 .select('street_name')
                                 ).collect()]
    return data


def get_cities_names(spark_session: SparkSession) -> list:
    """
    Загружаем список городов из CSV

    :param spark_session:  спарк-сессия
    :return: cities: список городов
    """
    cities = [row[0] for row in (spark_session.read.format('csv').option('header', 'true')
                                 .load('./data/city.csv')
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
    """
    Создаем DataFrame с данными

    :param spark_session: спарк-сессия
    :param cities: список городов
    :param streets: список улиц
    :param record_num: число записей на генерацию
    :return:
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True),
        StructField("price", IntegerType(), True),
    ])
    data = (generate_random_data(record_num=record_num, cities=cities, streets=streets))
    dataframe = spark_session.createDataFrame(data, schema)
    return dataframe.withColumn("id", monotonically_increasing_id())


app = Flask(__name__)


@app.route('/generate_and_produce_new_data', methods=['GET'])
def generate_and_produce_new_data():
    spark = create_spark_session(app_name="Producer Emulator")
    streets_names = get_streets_names()
    if streets_names is None:
        logger.error("Mockaroo API error")
        streets_names = get_streets_names_archives(spark_session=spark)
    cities_names = get_cities_names(spark_session=spark)
    record_num = random.randint(20, 100)
    generated_df = create_data(spark, cities=cities_names, streets=streets_names, record_num=record_num)
    message = producer_to_kafka(data=generated_df, topic="new_data", host="localhost", port=9092, logger=logger)
    logger.info(message)
    return {'message': message}


schedule.every(120).seconds.do(generate_and_produce_new_data)


@app.route('/generate_and_produce_requests', methods=['GET'])
def generate_and_produce_requests():
    spark = (SparkSession.builder
             .appName("Requests Emulator")
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    streets_names = get_streets_names()
    if streets_names is None:
        logger.error("Mockaroo API error")
        streets_names = get_streets_names_archives(spark_session=spark)
    cities_names = get_cities_names(spark_session=spark)
    record_num = random.randint(2, 5)
    generated_df = (create_data(spark, cities=cities_names, streets=streets_names, record_num=record_num)
                    .select("id", "city", "street", "floor", "rooms")
                    )
    message = producer_to_kafka(data=generated_df, topic="requests", host="localhost", port=9092, logger=logger)
    logger.info(message)
    return {'message': message}


@app.route('/status', methods=['GET'])
def status():
    return jsonify({'message': 'Service is running'})


if __name__ == "__main__":
    logger = create_logger()

    while True:
        schedule.run_pending()
        time.sleep(10)
        result_new_data = generate_and_produce_new_data()
        result_requests = generate_and_produce_requests()
