import pandas as pd
import psycopg2
from psycopg2 import sql

# Path to the CSV file
csv_file_path = './csv/sprint_results.csv'

# Read the CSV file using pandas
df = pd.read_csv(csv_file_path)

# Filter the data where raceid > 1120
filtered_df = df[df['raceId'] > 1120]

# Database connection parameters
db_params = {
    'dbname': 'f1',
    'user': 'postgres',
    'password': 'Milkyway@1',
    'host': '3.142.73.70',
    'port': '5432'
}


# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Define the table name
table_name = 'sprint_results'

# Create the table based on the filtered dataframe's columns
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
        ) for col in filtered_df.columns
    )
)

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

# Insert data from the filtered dataframe into the table
for index, row in filtered_df.iterrows():
    insert_query = sql.SQL("""
        INSERT INTO {table} ({fields})
        VALUES ({values})
    """).format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, filtered_df.columns)),
        values=sql.SQL(', ').join(sql.Placeholder() * len(filtered_df.columns))
    )
    cur.execute(insert_query, list(row))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
