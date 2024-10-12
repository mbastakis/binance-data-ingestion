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
            resample_freq = self.config['resample_frequency']  # e.g., '1T' for 1 minute

            # Ensure 'price' is numeric
            df['price'] = pd.to_numeric(df['price'], errors='coerce')

            # Drop rows with NaN prices after conversion
            initial_count = len(df)
            df = df.dropna(subset=['price'])
            dropped_count = initial_count - len(df)
            if dropped_count > 0:
                self.logger.warning(f"Dropped {dropped_count} rows due to non-numeric prices.")

            # Set 'timestamp' as the index for resampling
            df.set_index('timestamp', inplace=True)
            
            # Resample and calculate mean and median
            df_downsampled = df.resample(resample_freq).agg({'price': ['mean', 'median']})
            
            df_downsampled.columns = ['avg_price', 'median_price']

            # Drop rows with NaN values resulting from resampling
            df_downsampled.dropna(inplace=True)

            # Reset index to turn 'timestamp' back into a column
            df_downsampled.reset_index(inplace=True)

            # Assign 'symbol' column (assuming all rows in df have the same symbol)
            df_downsampled['symbol'] = df['symbol'].iloc[0]

            # Reorder columns to match the expected database schema
            df_downsampled = df_downsampled[['symbol', 'timestamp', 'avg_price', 'median_price']]

            return df_downsampled
