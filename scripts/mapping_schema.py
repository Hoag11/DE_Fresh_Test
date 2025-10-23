from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime
import logging

#logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
KAFKA_SERVER = 'localhost:9092'
TOPICS = ["appdb.accounts", "appdb.products", "appdb.sales_teams", "appdb.sales_pipeline"]

# MySQL configuration
MYSQL_BI_CONFIG = {
    "host": "mysql-bi",
    "user": "admin",
    "password": "admin",
    "database": "warehouse"
}

conn = mysql.connector.connect(**MYSQL_BI_CONFIG)
cursor = conn.cursor()

def process_dim_account():