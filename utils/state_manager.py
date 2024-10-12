import threading
import sqlite3

class StateManager:
    def __init__(self, db_path='state.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._initialize_db()

    def _initialize_db(self):
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ingestion_state (
                    symbol TEXT PRIMARY KEY,
                    collected_points INTEGER
                )
            ''')
            conn.commit()

    def get_collected_points(self, symbol):
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT collected_points FROM ingestion_state WHERE symbol = ?', (symbol,))
            result = cursor.fetchone()
            return result[0] if result else 0

    def update_collected_points(self, symbol, collected_points):
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO ingestion_state (symbol, collected_points)
                VALUES (?, ?)
                ON CONFLICT(symbol) DO UPDATE SET collected_points=excluded.collected_points
            ''', (symbol, collected_points))
            conn.commit()
