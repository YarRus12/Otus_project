import time
import schedule
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from flask import Flask

from utils import write_to_psql, create_logger, create_spark_session, kafka_consumer


def process_batch(df: DataFrame, epoch_id) -> None:
    """
    Обработка данных в пакете

    :param df: данные в пакете
    :param epoch_id:
    :return: None
    """
    columns = ["city", "street", "floor", "rooms", "price"]
    message = write_to_psql(df=df, table_name="STAGE.FLATS_TABLE", columns=columns)
    logger.info(message)


app = Flask(__name__)


@app.route('/start_kafka_consumer', methods=['GET'])
def start_kafka_consumer():
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("rooms", IntegerType(), True),
        StructField("price", LongType(), True)
    ])
    kafka_consumer(spark_session=spark, host="localhost",
                   port=9092, topic="new_data", schema=schema,
                   columns=["id", "city", "street", "floor", "rooms", "price"],
                   process_batch=process_batch)


if __name__ == "__main__":
    logger = create_logger()
    spark = create_spark_session(app_name="DataLoader")

    while True:
        schedule.run_pending()
        time.sleep(10)
        start_kafka_consumer()
