import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extras import Json

class DataLoader:
    def __init__(self, config):
        db_config = config['database']
        self.connection = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['dbname']
        )
        self._initialize_db()

    def _initialize_db(self):
        with self.connection.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS raw_data (
                    symbol TEXT,
                    data JSONB,
                    timestamp TIMESTAMPTZ,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
            self.connection.commit()

    def load_raw_data(self, symbol, data, timestamp):
        with self.connection.cursor() as cursor:
            cursor.execute('''
                INSERT INTO raw_data (symbol, data, timestamp)
                VALUES (%s, %s, to_timestamp(%s))
            ''', (symbol, Json(data), timestamp))  # Wrap data with Json
            self.connection.commit()
