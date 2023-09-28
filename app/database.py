import psycopg2
import csv
from flask import g
from config import Config
from tqdm import tqdm

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=Config.DATABASE_NAME,
            user=Config.DATABASE_USER,
            password=Config.DATABASE_PASSWORD,
            host=Config.DATABASE_HOST,
            port=Config.DATABASE_PORT
        )
        self.cursor = self.conn.cursor()

    def execute_query(self, query, params=None):
        # Execute a custom query and return the result
        if params is not None:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def create_tables(self):
        print("Creating tables...")
        # Create store_status table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS store_status (
                store_id TEXT,
                timestamp_utc TIMESTAMP,
                status TEXT
            )
        ''')

        # Create business_hours table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_hours (
                store_id TEXT,
                day INTEGER,
                start_time_local TIME,
                end_time_local TIME
            )
        ''')

        # Create store_timezone table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS store_timezone (
                store_id TEXT,
                timezone_str TEXT
            )
        ''')

        self.conn.commit()
        print("Tables created.")

    def insert_data(self, table, data):
        # Insert data into the specified table
        placeholders = ','.join(['%s'] * len(data))
        query = f'INSERT INTO {table} VALUES ({placeholders})'
        self.cursor.execute(query, data)
        self.conn.commit()

    def load_data_from_csv(self, table_name, file_path):
        print(f"Loading data into {table_name} from CSV...")
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header row

            # Use tqdm to create a progress bar
            for row in tqdm(csv_reader, unit=" rows", desc=f"Loading {table_name}"):
                if table_name == 'store_status':
                    store_id, status, timestamp_utc = row
                    self.insert_data(table_name, (store_id, timestamp_utc, status))
                elif table_name == 'business_hours':
                    store_id, day, start_time_local, end_time_local = row
                    self.insert_data(table_name, (store_id, day, start_time_local, end_time_local))
                elif table_name == 'store_timezone':
                    store_id, timezone_str = row
                    self.insert_data(table_name, (store_id, timezone_str))

        print(f"Data loaded into {table_name}.")


    def load_data_from_csvs(self):
        # Load data from store_status.csv
        self.load_data_from_csv('store_status', 'data/store_status.csv')

        # Load data from business_hours.csv
        self.load_data_from_csv('business_hours', 'data/business_hours.csv')

        # Load data from store_timezone.csv
        self.load_data_from_csv('store_timezone', 'data/store_timezone.csv')

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

def get_db():
    if 'db' not in g:
        g.db = Database()
    return g.db

def close_db(error):
    if hasattr(g, 'db'):
        g.db.close_connection()
