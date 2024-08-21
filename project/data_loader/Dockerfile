FROM debian:bullseye-slim
# Установите Python
RUN apt-get update && apt-get install -y python3 python3-pip
# Установите Java
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jdk
# Установка зависимостей
WORKDIR /app

COPY ../requirements.txt ./
COPY data_loader/ ./
COPY ../utils.py ./

# Установите необходимые пакеты для установки Java

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN pip install pip apscheduler
RUN echo "Установка зависимостей из requirements.txt..." && \
    pip install --no-cache-dir -r requirements.txt && \
    echo "Установка завершена." \