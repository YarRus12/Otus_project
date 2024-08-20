"""
Модуль обучает модель на данных, сохраненных за последний месяц
"""

import logging
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
logging.basicConfig(level=logging.DEBUG)

spark_jars_packages = ",".join(["org.postgresql:postgresql:42.2.24"])


def get_data_psql(spark_session: SparkSession) -> DataFrame:
    host = 'localhost'
    port = 5435
    database = 'docker_app_db'
    table_name = "STAGE.FLATS_TABLE"
    pg_df = (spark_session.read
             .format("jdbc")
             .option("dbtable", table_name)
             .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
             .option("user", "docker_app")
             .option("password", "docker_app")
             .option("driver", "org.postgresql.Driver")
             .load()
             )
    if pg_df.take(1):
        return pg_df


def vector_assembler(features_columns: list) -> VectorAssembler:
    """
    Функция создает векторизатор признаков для построения модели
    """
    chosen_columns = [x for x in features_columns if x not in ('price', 'city', 'street', 'key', 'created_at')]
    logger.info(chosen_columns)
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
        .addGrid(lr.elasticNetParam, [0.0, 0.3, 1.0]) \
        .build()

    # Настройка TrainValidationSplit
    return TrainValidationSplit(estimator=lr,
                                estimatorParamMaps=paramGrid,
                                evaluator=RegressionEvaluator(labelCol="price", predictionCol="prediction",
                                                              metricName="rmse"),
                                trainRatio=0.8)


def data_preparation(in_dataframe: DataFrame) -> DataFrame:
    city_index = StringIndexer(inputCol='city', outputCol="city_index", handleInvalid="keep")
    street_index = StringIndexer(inputCol='street', outputCol="street_index", handleInvalid="keep")
    indexed_dataframe = city_index.fit(in_dataframe).transform(in_dataframe)
    indexed_dataframe = street_index.fit(indexed_dataframe).transform(indexed_dataframe)
    assembler = vector_assembler(
        features_columns=indexed_dataframe.columns,
    )
    res_dataframe = assembler.transform(indexed_dataframe)
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
    # Оценка RMSE
    evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='price', metricName='rmse')
    rmse = evaluator.evaluate(predictions)
    # Оценка MAE
    evaluator_mae = RegressionEvaluator(predictionCol='prediction', labelCol='price', metricName='mae')
    mae = evaluator_mae.evaluate(predictions)
    # Оценка R²
    evaluator_r2 = RegressionEvaluator(predictionCol='prediction', labelCol='price', metricName='r2')
    r2 = evaluator_r2.evaluate(predictions)
    logger.info(f"RMSE: {rmse}, MAE: {mae}, R²: {r2}")

    return p_model


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    running_date = datetime.now().date()
    spark = (SparkSession.builder
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", spark_jars_packages)
             .getOrCreate()
             )

    # Получение данных из PostgreSQL
    df = get_data_psql(spark_session=spark)
    df = (df.filter(col('created_at') >= running_date-timedelta(weeks=4))
          .filter(col('city').isNotNull() & col('street').isNotNull())
          .drop('key', 'created_at')
          )
    prepared_model = train_model(dataframe=df)
    prepared_model.write().overwrite().save("./models")

    spark.stop()
