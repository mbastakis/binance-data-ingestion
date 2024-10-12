from sqlalchemy import create_engine, Table, MetaData, text, Column, String, TIMESTAMP, Float, MetaData, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
from utils.config_loader import ConfigLoader
from utils.logger import get_logger

class DataTransformer:
    def __init__(self, config):
        self.config = config
        db_config = config['database']
        self.logger = get_logger(self.__class__.__name__)
        self.engine = self._create_db_engine(db_config)
        self._initialize_db()
    
    def _create_db_engine(self, db_config):
        db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@" \
                 f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        return create_engine(db_url)

    def _initialize_db(self):
        try:
            metadata = MetaData()
            downsampled_table = Table(
                'downsampled_data', metadata,
                Column('symbol', String, nullable=False),
                Column('timestamp', TIMESTAMP(timezone=True), nullable=False),
                Column('avg_price', Float),
                PrimaryKeyConstraint('symbol', 'timestamp')
            )
            metadata.create_all(self.engine)
            self.logger.info("downsampled_data table initialized or already exists.")
        except Exception as e:
            self.logger.error(f"Error initializing downsampled_data table: {e}")


    
    def transform_data(self):
        self.logger.info("Starting data transformation...")
        for symbol in self.config['symbols']:
            try:
                df = self._fetch_raw_data(symbol)
                if not df.empty:
                    df_downsampled = self._downsample_data(df)
                    self._store_downsampled_data(symbol, df_downsampled)
                    self.logger.info(f"Transformed and stored data for {symbol}")
                else:
                    self.logger.info(f"No new data to transform for {symbol}")
            except Exception as e:
                self.logger.error(f"Error transforming data for {symbol}: {e}")

    def _fetch_raw_data(self, symbol):
        query = text('''
            SELECT timestamp, data->>'price' AS price
            FROM raw_data
            WHERE symbol = :symbol
            AND timestamp > (
                SELECT COALESCE(MAX(timestamp), '1970-01-01') FROM downsampled_data WHERE symbol = :symbol
            )
            ORDER BY timestamp ASC
        ''')
        params = {'symbol': symbol}
        df = pd.read_sql_query(query, self.engine, params=params)
        if df.empty:
            return df
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['price'] = pd.to_numeric(df['price'])
        return df

    def _downsample_data(self, df):
        resample_freq = self.config['resample_frequency']
        df.set_index('timestamp', inplace=True)
        # Resample data to the specified frequency, computing the average price
        df_downsampled = df.resample(resample_freq).mean().dropna()
        return df_downsampled

    def _store_downsampled_data(self, symbol, df_downsampled):
        # Prepare the data for insertion
        df_downsampled.reset_index(inplace=True)
        df_downsampled['symbol'] = symbol
        df_downsampled.rename(columns={'price': 'avg_price'}, inplace=True)
        # Reorder columns to match the table schema
        df_downsampled = df_downsampled[['symbol', 'timestamp', 'avg_price']]
        
        # Use SQLAlchemy Core for insertion with conflict handling
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        downsampled_table = metadata.tables['downsampled_data']
        
        records = df_downsampled.to_dict(orient='records')
        stmt = pg_insert(downsampled_table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'timestamp'])
        
        with self.engine.begin() as connection:
            connection.execute(stmt)
