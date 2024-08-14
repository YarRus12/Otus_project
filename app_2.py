from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession

# Укажите путь к директории, где вы сохранили модель
model_path = "./models"  # Путь к директории, где сохранена модель
# Загрузка модели
loaded_model = PipelineModel.load(model_path)
# Подтверждение успешной загрузки
print("Model loaded successfully.")

"""
+-----------+-----------------+-----+-----+-------+
|       city|           street|floor|rooms|  price|
+-----------+-----------------+-----+-----+-------+
|Краснокамск|     Lenin Avenue|   18|    4|4548908|
|   Сердобск|   Lenin Prospect|    9|    2|2413687|
|    Сычевка|   Victory Street|   13|    2|1520498|
"""

spark = SparkSession.builder.appName("My App").getOrCreate()

data = [
    ("Краснокамск", "Lenin Avenue", 18, 4, 4548908),
    ("Сердобск", "Lenin Prospect", 9, 2, 2413687),
    ("Сычевка", "Victory Street", 13, 2, 1520498)
]

df = spark.createDataFrame(data, ["city", "street", "floor", "rooms", "price"])
df.show()

take_df = df.select("city", "street", "floor", "rooms")

# Apply the same transformations as during training
city_indexer = StringIndexer(inputCol='city', outputCol='city_index')
street_indexer = StringIndexer(inputCol='street', outputCol='street_index')

# Fit the indexers on the new data (this is necessary to create the index mapping)
city_indexer_model = city_indexer.fit(df)
street_indexer_model = street_indexer.fit(df)

# Transform the data
indexed_df = city_indexer_model.transform(df)
indexed_df = street_indexer_model.transform(indexed_df)

# Assemble features
assembler = VectorAssembler(inputCols=["city_index", "street_index", "floor", "rooms"], outputCol="features")
final_df = assembler.transform(indexed_df)

# Now you can use the loaded model to make predictions
result = loaded_model.transform(final_df)
result.show()

spark.stop()