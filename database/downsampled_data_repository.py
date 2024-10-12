# database/downsampled_data_repository.py

from database.models import DownsampledData, Base
from database.database import Database
from sqlalchemy.dialects.postgresql import insert as pg_insert


class DownsampledDataRepository:
    def __init__(self):
        self.engine = Database.get_engine()
        Base.metadata.create_all(self.engine)

    def insert_downsampled_data(self, df_downsampled):
        session = Database.get_session()
        try:
            df_downsampled.rename(columns={'price': 'avg_price'}, inplace=True)
            df_downsampled = df_downsampled[['symbol', 'timestamp', 'avg_price', 'median_price']]

            records = df_downsampled.to_dict(orient='records')
            stmt = pg_insert(DownsampledData.__table__).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['symbol', 'timestamp']
            )
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
