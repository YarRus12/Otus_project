"""
Модуль обучает модель на сохраненных за последний месяц данных
"""
import os
import pickle

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


def vector_assembler(features_columns: list) -> VectorAssembler:
    """
    Функция создает векторизатор признаков для построения модели
    """
    chosen_columns = [x for x in features_columns if x not in ('price', 'city', 'street', 'key', 'created_at')]
    print(chosen_columns)
    features = VectorAssembler(inputCols=chosen_columns, outputCol="features")
    return features


def build_random_forest() -> RandomForestRegressor:
    """
    Функция создает модель случайного леса
    """
    return RandomForestRegressor(labelCol="price", featuresCol="features")


def build_evaluator() -> RegressionEvaluator:
    """
    Функция расчитывает метрику среднеквардратичной ошибки
    """
    return RegressionEvaluator(predictionCol='prediction',
                               labelCol='price',
                               metricName='rmse')


def build_tvs(lr) -> TrainValidationSplit:
    """
    Функция создает TrainValidationSplit для линейной регрессии
    """
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.1, 0.01]) \
        .addGrid(lr.fitIntercept, [False, True]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    # Настройка TrainValidationSplit
    return TrainValidationSplit(estimator=lr,
                                 estimatorParamMaps=paramGrid,
                                 evaluator=RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse"),
                                 trainRatio=0.8)


def data_preparation(in_dataframe: DataFrame) -> DataFrame:
    city_index = StringIndexer(inputCol='city', outputCol="city_index")
    street_index = StringIndexer(inputCol='street', outputCol="street_index")
    indexed_dataframe = city_index.fit(in_dataframe).transform(in_dataframe)
    indexed_dataframe = street_index.fit(indexed_dataframe).transform(indexed_dataframe)
    assembler = vector_assembler(
        features_columns=indexed_dataframe.columns,
    )
    res_dataframe = assembler.transform(indexed_dataframe)  #.select('city_index', 'street_index', 'floor', 'rooms')
    return res_dataframe


def train_model(dataframe: DataFrame):
    # Разделение данных на обучающую и тестовую выборки
    # Приведение категориальных признаков обучающей выборки к числовому виду и векторизация признаков
    vectorized_dataframe = data_preparation(in_dataframe=dataframe)

    train_df, test_data = vectorized_dataframe.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(featuresCol='features', labelCol='price')
    tvs = build_tvs(lr)

    models = tvs.fit(train_df)
    best = models.bestModel
    pipeline = Pipeline(stages=[best])
    # Обучение конвейера на обучающих данных
    p_model = pipeline.fit(train_df)

    # Оценка модели на тестовых данных
    predictions = p_model.transform(test_data)
    evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='price', metricName='rmse')
    rmse = evaluator.evaluate(predictions)
    print(f"RMSE: {rmse}")
    return p_model


if __name__ == "__main__":
    running_date = datetime.now().date()
    spark = SparkSession.builder.getOrCreate()

    from producer_emulator import *

    streets_names = get_streets_names()
    cities_names = get_cities_names(spark_session=spark)
    df = (create_data(spark, cities=cities_names, streets=streets_names)
          .withColumn("key", monotonically_increasing_id())
          )

    # df = (spark.table('FLATS_HIVE_TABLE').filter(col('created_at') > running_date - timedelta(weeks=4))
    #       )

    df = df.drop('key', 'created_at').filter(col('city').isNotNull() & col('street').isNotNull())
    prepared_model = train_model(dataframe=df)
    prepared_model.write().overwrite().save("./models")

    spark.stop()
