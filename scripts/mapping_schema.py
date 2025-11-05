from kafka import KafkaConsumer
import json
import pymysql
from datetime import datetime, timedelta
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

KAFKA_BROKER = "kafka:9092"
TOPICS = [
    "appdb.app.accounts",
    "appdb.app.products",
    "appdb.app.sales_teams",
    "appdb.app.sales_pipeline",
]

MYSQL_BI = {
    "host": "mysql-bi",
    "user": "admin",
    "password": "admin",
    "database": "warehouse",
    "port": 3306,
    "cursorclass": pymysql.cursors.DictCursor,
}

conn = pymysql.connect(**MYSQL_BI)
cur = conn.cursor()
logging.info("Connected to MySQL BI database")

def parse_date(val):
    """Chuyển đổi giá trị ngày từ Debezium (epoch days hoặc str) về datetime hoặc None."""
    if val is None:
        logging.warning("Received None for date field.")
        return None
    if isinstance(val, (int, float)):  # epoch days
        if val == 0:
            logging.warning(f"Received 0 as date value, returning None: {val}")
            return None  # Tránh cộng với 0
        try:
            # Cộng số ngày vào mốc thời gian 1970-01-01
            date = datetime(1970, 1, 1) + timedelta(days=val)
            logging.info(f"Parsed date from epoch days {val}: {date}")
            return date
        except Exception as e:
            logging.error(f"Error parsing epoch days: {val} -> {e}")
            return None
    if isinstance(val, str):
        try:
            # Trường hợp ngày dạng string
            date = datetime.strptime(val.split("T")[0], "%Y-%m-%d")
            logging.info(f"Parsed date from string {val}: {date}")
            return date
        except ValueError:
            try:
                # ISO format với timezone
                date = datetime.fromisoformat(val.replace("Z", ""))
                logging.info(f"Parsed date from ISO {val}: {date}")
                return date
            except Exception as e:
                logging.error(f"Error parsing date string {val}: {e}")
                return None
    logging.warning(f"Invalid date format: {val}")
    return None



def lookup_or_insert_account(data):
    cur.execute(
        "SELECT account_id FROM dim_account WHERE account_name = %s",
        (data.get("account"),),
    )
    account = cur.fetchone()
    if not account:
        query = """
            INSERT INTO dim_account (account_name, sector, year_established, employees, office_location, subsidiary_of)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(
            query,
            (
                data.get("account"),
                data.get("sector"),
                data.get("year_established"),
                data.get("employees"),
                data.get("office_location"),
                data.get("subsidiary_of"),
            ),
        )
        conn.commit()
        cur.execute(
            "SELECT account_id FROM dim_account WHERE account_name = %s",
            (data.get("account"),),
        )
        account = cur.fetchone()
        logging.info(f"Inserted new account: {data.get('account')}")
    return account["account_id"]


def lookup_or_insert_product(data):
    cur.execute(
        "SELECT product_id FROM dim_product WHERE product_name = %s",
        (data.get("product"),),
    )
    product = cur.fetchone()
    if not product:
        cur.execute(
            """
            INSERT INTO dim_product (product_name, series, sale_price)
            VALUES (%s, %s, %s)
            """,
            (data.get("product"), data.get("series"), data.get("sales_price")),
        )
        conn.commit()
        cur.execute(
            "SELECT product_id FROM dim_product WHERE product_name = %s",
            (data.get("product"),),
        )
        product = cur.fetchone()
        logging.info(f"Inserted new product: {data.get('product')}")
    return product["product_id"]


def lookup_or_insert_sales_agent(data):
    sales_agent = data.get("sales_agent")
    cur.execute(
        "SELECT sales_agent_id FROM dim_sales_agent WHERE sale_agent = %s",
        (sales_agent,),
    )
    agent = cur.fetchone()
    if not agent:
        cur.execute(
            """
            INSERT INTO dim_sales_agent (sale_agent, manager, regional_office)
            VALUES (%s, %s, %s)
            """,
            (sales_agent, data.get("manager"), data.get("regional_office")),
        )
        conn.commit()
        cur.execute(
            "SELECT sales_agent_id FROM dim_sales_agent WHERE sale_agent = %s",
            (sales_agent,),
        )
        agent = cur.fetchone()
        logging.info(f"Inserted new sales agent: {sales_agent}")
    return agent["sales_agent_id"]


def insert_fact_sales(data):
    account_id = lookup_or_insert_account(data)
    product_id = lookup_or_insert_product(data)
    sales_agent_id = lookup_or_insert_sales_agent(data)

    engage_date = parse_date(after.get("engage_date"))
    close_date = parse_date(after.get("close_date"))

    deal_stage = data.get("deal_stage")
    close_value = data.get("close_value")

    deal_duration_days = (
        (close_date - engage_date).days if close_date and engage_date else None
    )
    conversion_rate = 1.0 if deal_stage == "Won" else 0.0

    cur.execute(
        """
        INSERT INTO fact_accounts (account_id, product_id, sales_agent_id, deal_stage, engage_date, close_date,
                                   close_value, deal_duration_days, conversion_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            account_id,
            product_id,
            sales_agent_id,
            deal_stage,
            engage_date,
            close_date,
            close_value,
            deal_duration_days,
            conversion_rate,
        ),
    )
    conn.commit()


consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mapping",
)


for msg in consumer:
    topic = msg.topic
    value = msg.value

    payload = value.get("payload", {})
    op = payload.get("op")
    after = payload.get("after")

    # Bỏ qua sự kiện không có dữ liệu (delete, schema, heartbeat)
    if not after or op not in ("c", "u"):
        continue

    try:
        if "accounts" in topic:
            lookup_or_insert_account(after)
        elif "products" in topic:
            lookup_or_insert_product(after)
        elif "sales_teams" in topic:
            lookup_or_insert_sales_agent(after)
        elif "sales_pipeline" in topic:
            insert_fact_sales(after)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error processing message from topic {topic}: {e}")
