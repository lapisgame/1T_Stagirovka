import csv
import psycopg2
import progressbar

# Подключение к базе данных PostgreSQL
conn = psycopg2.connect(
    host="146.120.224.155",
    port="10202",
    database="4DB",
    user="4_group",
    password="4_group"
)

# Создание курсора для выполнения SQL-запросов
cur = conn.cursor()

dele = """DROP TABLE IF EXISTS rabota1000_lapis;"""
cur.execute(dele)
conn.commit()

create = """CREATE TABLE IF NOT EXISTS rabota1000_lapis(
    id serial PRIMARY KEY,
    vac_link text,
    name text,
    city text,
    company text,
    experience varchar(20),
    schedule varchar(20),
    employment varchar(20),
    skills text,
    description text,
    salary text,
    time text,
    salary_from text,
    salary_to text,
    salary_currency text
);"""
cur.execute(create)
conn.commit()

row_count = 0
with open('../../rabota1000/async_pars.csv', 'r', encoding='utf-8') as file:
    csv_data = csv.reader(file, delimiter=';')
    row_count = sum(1 for row in csv_data)

# Открытие CSV файла для чтения
with open('../../rabota1000/async_pars.csv', 'r', encoding='utf-8') as file:
    csv_data = csv.reader(file, delimiter=';')
    bar = progressbar.ProgressBar(maxval=row_count).start()
    # Пропуск заголовка, если есть
    next(csv_data)
    index = 0
    # Цикл по строкам CSV файла
    for row in csv_data:
        index += 1
        # Извлечение данных из строки
        col1 = row[0]
        col2 = row[1]
        col3 = row[2]
        col4 = row[3]
        col5 = row[4]
        col6 = row[5]
        col7 = row[6]
        col8 = row[7]
        col9 = row[8]
        col10 = row[9][0:65000]
        col11 = row[10]
        col12 = row[11]
        col13 = row[12]
        col14 = row[13]
        
        # Формирование и выполнение SQL-запроса для вставки данных в таблицу
        sql = "INSERT INTO rabota1000_lapis (vac_link, name, city, company, experience, schedule, employment, skills, description, salary, time, salary_from, salary_to, salary_currency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        values = (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14)
        cur.execute(sql, values)
        
        bar.update(index)
        if index % 500 == 0:
            conn.commit()

# Фиксация изменений и закрытие соединения с базой данных
conn.commit()
cur.close()
conn.close()