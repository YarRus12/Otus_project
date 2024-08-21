FROM python:3.10

# Установка зависимостей
WORKDIR /app

COPY requirements.txt ./
COPY model_application/ ./
COPY ../utils.py ./

RUN apt-get update && apt-get install -y python3-pip
RUN pip install pip apscheduler
RUN echo "Установка зависимостей из requirements.txt..." && \
    pip install --no-cache-dir -r requirements.txt && \
    echo "Установка завершена." \



# Запуск приложения
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=8083"]