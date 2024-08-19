import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="docker_app",
    password="docker_app",
    database="docker_app_db"
)

if conn:
    print("Подключение успешно")
else:
    print("Ошибка подключения")