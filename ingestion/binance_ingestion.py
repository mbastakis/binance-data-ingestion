from ingestion.base_ingestion import DataIngestionClient
from binance.spot import Spot as Client
from utils.rate_limiter import RateLimiter
from utils.state_manager import StateManager
from utils.logger import get_logger
from datetime import datetime, timezone
from database.raw_data_repository import RawDataRepository
# import time


class BinanceIngestionClient(DataIngestionClient):
    def __init__(self, config):
        self.config = config
        self.symbols = config['symbols']
        self.data_points = config['data_points']
        self.api_rate_limit = config['api_rate_limit']
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        self.client = Client(self.api_key, self.api_secret)
        self.rate_limiter = RateLimiter(self.api_rate_limit)
        self.state_manager = StateManager()
        self.raw_data_repo = RawDataRepository()
        self.logger = get_logger(self.__class__.__name__)

    def ingest_data(self, symbol):
        """Ingest data for a single symbol."""
        # start_time = time.time()
        collected_points = self.state_manager.get_collected_points(symbol)
        if collected_points >= self.data_points:
            return

        with self.rate_limiter:
            try:
                self.logger.debug(f"Requesting data for {symbol}...")
                data = self.client.ticker_price(symbol=symbol)
                timestamp = datetime.now(timezone.utc)
                self.raw_data_repo.insert_raw_data(symbol, data, timestamp)
                collected_points = self.state_manager.update_collected_points(symbol)
                self.logger.info(f"Collected data point {collected_points} for {symbol}")

                if collected_points >= self.data_points:
                    self.logger.info(f"Reached data points limit for {symbol}")
            except Exception as e:
                self.logger.error(f"Error processing data for {symbol}: {e}")
        # end_time = time.time()
        # elapsed_time = end_time - start_time
        # self.logger.debug(f"Time taken for {symbol}: {elapsed_time:.2f} seconds")
