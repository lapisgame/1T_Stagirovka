import psycopg2
import csv

conn = psycopg2.connect("host='146.120.224.155' port='10202' dbname='4DB' user='4_group' password='4_group'")
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS rabota1000_lapis_test(
        id serial PRIMARY KEY,
        vac_link varchar(100),
        name varchar(100),
        city varchar(40),
        company varchar(40),
        experience varchar(20),
        schedule varchar(20),
        employment varchar(20),
        skills varchar(400),
        description text,
        salary varchar(25),
        time varchar(50),
        salary_from varchar(30),
        salary_to varchar(30),
        salary_currency varchar(30)           
)
""")
conn.commit()

with open('../../rabota1000/async_pars.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    id = 0
    count = 0
    for row in reader:
        try:
            id += 1
            row.insert(0, str(id))
            row[9] = (row[9])[0:65530]+'...'
            cur.execute(
                "INSERT INTO rabota1000_lapis_test VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                row
            )
            conn.commit()
        except Exception as e:
            count += 1
            print(id, end=' ')
print(count)
conn.commit()