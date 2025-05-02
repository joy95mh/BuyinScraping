import psycopg2
from datetime import datetime
from src.utils.db_handler import get_db_connection

class DatabasePipeline:
    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraped_products (
                id SERIAL PRIMARY KEY,
                bn_code TEXT,
                price_link TEXT,
                xpath_result TEXT,
                out_of_stock TEXT,
                timestamp TIMESTAMP
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        self.cursor.execute("""
            INSERT INTO scraped_products (bn_code, price_link, xpath_result, out_of_stock, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            item["bn_code"],
            item["price_link"],
            item["xpath_result"],
            item["out_of_stock"],
            datetime.now()
        ))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()