# transformation/transformer.py

from database.raw_data_repository import RawDataRepository
from database.downsampled_data_repository import DownsampledDataRepository
from utils.logger import get_logger
import pandas as pd


class DataTransformer:
    def __init__(self, config):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.raw_data_repo = RawDataRepository()
        self.downsampled_repo = DownsampledDataRepository()

    def transform_data(self):
        self.logger.info("Starting data transformation...")
        for symbol in self.config['symbols']:
            try:
                df = self.raw_data_repo.fetch_unprocessed_data(symbol)
                if not df.empty:
                    df_downsampled = self._downsample_data(df)
                    self.downsampled_repo.insert_downsampled_data(df_downsampled)
                    self.logger.info(f"Transformed and stored data for {symbol}")
                else:
                    self.logger.info(f"No new data to transform for {symbol}")
            except Exception as e:
                self.logger.error(f"Error transforming data for {symbol}: {e}")

    def _downsample_data(self, df):
        resample_freq = self.config['resample_frequency']
        df.set_index('timestamp', inplace=True)
        df_downsampled = df.resample(resample_freq).mean().dropna()
        df_downsampled.reset_index(inplace=True)
        df_downsampled['symbol'] = df['symbol'].iloc[0]  # Ensure 'symbol' column exists
        return df_downsampled
