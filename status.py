import pandas as pd
import psycopg2
from psycopg2 import sql

csv_file_path = './status.csv'

df = pd.read_csv(csv_file_path)

db_params = {
    'dbname': 'f1',
    'user': 'postgres',
    'password': 'Milkyway@1',
    'host': '3.142.73.70',
    'port': '5432'
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

table_name = 'status'

create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table} (
        {fields}
    )
""").format(
    table=sql.Identifier(table_name),
    fields=sql.SQL(', ').join(
        sql.SQL("{} {}").format(
            sql.Identifier(col),
            sql.SQL("VARCHAR")
        ) for col in df.columns
    )
)

cur.execute(create_table_query)
conn.commit()

for index, row in df.iterrows():
    insert_query = sql.SQL("""
        INSERT INTO {table} ({fields})
        VALUES ({values})
    """).format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, df.columns)),
        values=sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
    )
    cur.execute(insert_query, list(row))

conn.commit()

cur.close()
conn.close()
