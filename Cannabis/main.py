import io
import requests
import psycopg2
import pandas as pd

# Входные данные для подключния к БД GreenPlum

dbname = '<your_dbname>'
host = '<hostname>'
port = '<port>'
user = '<your_login>'
password = '<your_password>'

# Парсим данные с api

response = requests.get(url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10')
data = pd.DataFrame(data = response.json())
data['loading_dt'] = pd.to_datetime(datetime.datetime.now().replace(microsecond = 0))

# Вспомогательные функции для работы с GreenPlum

def select_table_from_gp(sql_query):
    conn = psycopg2.connect(
			    host = host,
			    port = port,
			    dbname = dbname,
			    user = user,
			    password = password
			   )
    cur = conn.cursor()
    cur.execute(sql_query)
    columns = cur.description
    result = cur.fetchall()
    return pd.Dataframe(data = result, columns = [column[0] for column in columns])

def create_table_on_gp(sql_query):
    conn = psycopg2.connect(
			    host = host,
			    port = port,
			    dbname = dbname,
			    user = user,
			    password = password
			   )    
    cur = conn.cursor()
    cur.execute(sql_query)
    conn.commit()
    return

def insert_into_table_on_gp(dataframe):
    csv_io = io.StringIO()
    dataframe.to_csv(path_or_buf = csv_io, sep = '\t', index = False, header = False)
    csv_io.seek(0)
    conn = psycopg2.connect(
			    host = host,
			    port = port,
			    dbname = dbname,
			    user = user,
			    password = password
			   )    
    cur = conn.cursor()
    cur.copy_from(file = csv_io, table = 'stg.cannabis', sep = '\t')
    conn.commit()
    return

# Создадим таблицу в БД

create_table = """
    create table if not exists stg.cannabis
    (
        id text, 
	uid text, 
	strain text, 
	cannabinoid_abbreviation text, 
	cannabinoid text,
	terpene text, 
	medical_use text, 
	health_benefit text, 
	category text, 
	type text,
	buzzword text, 
	brand text, 
	loading_dt timestamp
    )
    with 
    (
	appendonly = true,
	compresstype = zstd,
	orientation = column,
	compresslevel = 1
    )
    distributed by (uid)
"""

create_table_on_gp(create_table)

# Выполянем вставку данных в таблицу

insert_into_table_on_gp(data)