from datetime import datetime

from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json, struct, col, from_json
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
import logging
logging.basicConfig(level=logging.DEBUG)



spark_jars_packages = ",".join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", ])


def producer_to_kafka(data: DataFrame, topic: str, host: str, port: int) -> str:
    kafka_options = {
        "kafka.bootstrap.servers": f"{host}:{port}",
        "topic": topic
    }
    data = data.cache()
    num_rows = data.count()
    (data
     .select(to_json(struct(*[col(c) for c in data.columns])).alias("value"))
     .write
     .format("kafka")
     .options(**kafka_options)
     .save()
     )
    data.unpersist()
    return f'{num_rows} rows were send to {kafka_options["topic"]} successfully at {datetime.now()}'


def execute_model(path, dataframe):
    loaded_model = PipelineModel.load(path)

    # Подтверждение успешной загрузки
    logger.info("Model loaded successfully.")
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
    dataframe.show()
    if dataframe.take(1):
        result = execute_model(path="./models", dataframe=dataframe)
        message = producer_to_kafka(data=result, topic="answers", host="localhost", port=9092)
        logger.info(message)


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
        StructField("rooms", IntegerType(), True)
    ])

    df = (spark_session.readStream
          .format("kafka")
          .option("subscribe", topic)
          .options(**kafka_options)
          .load()
          )
    processed_df = df.select(
        col("key"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("key", "data.*")

    processed_df = processed_df.select("id", "city", "street", "floor", "rooms")

    query = processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    spark = (SparkSession.builder
             .appName("ModelApplication")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", spark_jars_packages)
             .getOrCreate())
    # Укажите путь к директории, где вы сохранили модель
    model_path = "../models"  # Путь к директории, где сохранена модель

    kafka_consumer(spark_session=spark, host="localhost", port=9092, topic="new_data")
    spark.stop()
