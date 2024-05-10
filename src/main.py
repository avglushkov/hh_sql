
import psycopg2

# Создайте подключение к базе данных
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='hh_db',
    user='hh_bd_user',
    password='654321')

# Открытие курсора
cur = conn.cursor()


self, id, name, url, salary, address, employer, snippet