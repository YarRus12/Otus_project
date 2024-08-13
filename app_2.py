from pyspark.ml import PipelineModel

# Укажите путь к директории, где вы сохранили модель
model_path = "flats_model"  # Путь к директории, где сохранена модель

# Загрузка модели
loaded_model = PipelineModel.load(model_path)

# Подтверждение успешной загрузки
print("Model loaded successfully.")