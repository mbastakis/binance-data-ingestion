import threading
import time
from concurrent.futures import ThreadPoolExecutor
from binance.spot import Spot as Client
from ingestion.base_ingestion import DataIngestionClient
from utils.rate_limiter import RateLimiter
from utils.config_loader import ConfigLoader
from utils.state_manager import StateManager
from loading.loader import DataLoader
from utils.logger import get_logger
from datetime import datetime, timezone  # Add this import


class BinanceIngestionClient(DataIngestionClient):
    def __init__(self, config):
        self.config = config
        self.symbols = config['symbols']
        self.sampling_frequency = config['sampling_frequency']
        self.data_points = config['data_points']
        self.api_rate_limit = config['api_rate_limit']
        self.max_workers = config['max_workers']
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        self.client = Client(self.api_key, self.api_secret)
        self.rate_limiter = RateLimiter(self.api_rate_limit)
        self.state_manager = StateManager()
        self.data_loader = DataLoader(config)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.stop_event = threading.Event()
        self.logger = get_logger(self.__class__.__name__)

    def start_ingestion(self):
        """Start the data ingestion process for all symbols."""
        for symbol in self.symbols:
            self.executor.submit(self.ingest_data, symbol)

    def stop_ingestion(self):
        """Stop the data ingestion process."""
        self.stop_event.set()
        self.executor.shutdown(wait=True)
        self.logger.info("Ingestion stopped.")

    def ingest_data(self, symbol):
        """Ingest data for a single symbol."""
        collected_points = self.state_manager.get_collected_points(symbol)
        while collected_points < self.data_points and not self.stop_event.is_set():
            with self.rate_limiter:
                try:
                    data = self.client.ticker_price(symbol=symbol)
                    # Convert timestamp to datetime with timezone
                    timestamp = datetime.now(timezone.utc)
                    # Save data
                    self.data_loader.load_raw_data(symbol, data, timestamp)
                    # Update state
                    collected_points += 1
                    self.state_manager.update_collected_points(symbol, collected_points)
                    self.logger.info(f"Collected data point {collected_points} for {symbol}")
                except Exception as e:
                    self.logger.error(f"Error processing data for {symbol}: {e}")
                finally:
                    time.sleep(self.sampling_frequency)
