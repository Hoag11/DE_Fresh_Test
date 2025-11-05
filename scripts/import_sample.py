import pandas as pd
import pymysql
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

conn = pymysql.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="app",
    port=3306,
    cursorclass=pymysql.cursors.DictCursor,
)

cur = conn.cursor()
logging.info("Connected to MySQL App database")

sample_data = {
    "sample_data/accounts.csv": "accounts",
    "sample_data/products.csv": "products",
    "sample_data/sales_teams.csv": "sales_teams",
    "sample_data/sales_pipeline.csv": "sales_pipeline",
}

for file, table in sample_data.items():
    df = pd.read_csv(file)

    df = df.replace([float("nan"), float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)

    logging.info(f"Inserted data into {table} from {file}")

    for _, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join(row.index)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        try:
            cur.execute(sql, tuple(row))
        except Exception as e:
            logging.error(f"Error inserting row into {table}: {e}")

    conn.commit()

cur.close()
conn.close()
logging.info("Load sample data completed")
logging.info("MySQL connection closed")
