

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("My App").getOrCreate()

data = [
    (1, "Краснокамск", "Lenin Avenue", 18, 4, 4548908),
    (2, "Сердобск", "Lenin Prospect", 9, 2, 2413687),
    (3, "Сычевка", "Victory Street", 13, 2, 1520498)
]
df = spark.createDataFrame(data, ["key", "city", "street", "floor", "rooms", "price"])

spark.sql("SHOW DATABASES").show()
