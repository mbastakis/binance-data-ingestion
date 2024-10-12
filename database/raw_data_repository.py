# database/raw_data_repository.py

from database.models import RawData, Base
from database.database import Database
from sqlalchemy import func, text
import pandas as pd


class RawDataRepository:
    def __init__(self):
        self.engine = Database.get_engine()
        Base.metadata.create_all(self.engine)

    def insert_raw_data(self, symbol, data, timestamp):
        session = Database.get_session()
        try:
            raw_data = RawData(symbol=symbol, data=data, timestamp=timestamp)
            session.add(raw_data)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def fetch_unprocessed_data(self, symbol):
        session = Database.get_session()
        try:
            query = text('''
                SELECT rd.timestamp, rd.data->>'price' AS price, rd.symbol
                FROM raw_data rd
                WHERE rd.symbol = :symbol
                AND rd.timestamp > (
                    SELECT COALESCE(MAX(timestamp), '1970-01-01') FROM downsampled_data WHERE symbol = :symbol
                )
                ORDER BY rd.timestamp ASC
            ''')
            params = {'symbol': symbol}
            df = pd.read_sql_query(query, session.bind, params=params)
            if df.empty:
                return df
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['price'] = pd.to_numeric(df['price'])
            return df
        finally:
            session.close()

    def delete_all_raw_data(self):
        session = Database.get_session()
        try:
            session.query(RawData).delete(synchronize_session=False)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
