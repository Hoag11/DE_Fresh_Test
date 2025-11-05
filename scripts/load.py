import random
from datetime import datetime, timedelta
import time
import logging
import pymysql

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MYSQL_APP = {
    "host": "localhost",
    "user": "admin",
    "password": "admin",
    "database": "app",
    "port": 3306,
    "cursorclass": pymysql.cursors.DictCursor,
}

conn = pymysql.connect(**MYSQL_APP)
cur = conn.cursor()
logging.info("Connected to MySQL App")

while True:
    account_name = f"Account_{random.randint(100, 999)}"
    cur.execute(
        """
        INSERT INTO accounts (account, sector, year_established, revenue, employees, office_location, subsidiary_of)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            account_name,
            random.choice(["finance", "retail", "technology"]),
            random.randint(1990, 2022),
            round(random.uniform(1e6, 5e6), 2),
            random.randint(10, 500),
            random.choice(["United States", "Germany", "Brazil"]),
            random.choice(["Acme Corporation", "Massive Dynamic", "Golddex"]),
        ),
    )

    product_name = f"Product_{random.randint(10, 99)}"
    cur.execute(
        """
        INSERT INTO products (product, series, sales_price)
        VALUES (%s, %s, %s)
        """,
        (
            product_name,
            f"Series-{random.randint(1, 5)}",
            round(random.uniform(100, 5000), 2),
        ),
    )

    agent_name = f"Agent_{random.randint(1, 50)}"
    cur.execute(
        """
        INSERT INTO sales_teams (sales_agent, manager, regional_office)
        VALUES (%s, %s, %s)
        """,
        (
            agent_name,
            f"Manager_{random.randint(1, 10)}",
            random.choice(["North", "South", "Central"]),
        ),
    )

    engage_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime(
        "%Y-%m-%d"
    )
    close_date = (datetime.now() + timedelta(days=random.randint(1, 15))).strftime(
        "%Y-%m-%d"
    )
    deal_stage = random.choice(["Won", "Lost", "Engaging"])
    close_value = random.randint(1000, 10000)

    cur.execute(
        """
        INSERT INTO sales_pipeline (opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            f"OPP_{random.randint(1000, 9999)}",
            agent_name,
            product_name,
            account_name,
            deal_stage,
            engage_date,
            close_date,
            close_value,
        ),
    )

    conn.commit()
    logging.info("Inserted 1 batch of fake data")
    time.sleep(5)
