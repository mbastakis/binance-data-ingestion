
from apscheduler.schedulers.background import BackgroundScheduler
from ingestion.binance_ingestion import BinanceIngestionClient
from transformation.transformer import DataTransformer
from utils.config_loader import ConfigLoader
from utils.logger import get_logger
from database.raw_data_repository import RawDataRepository


class Orchestrator:
    def __init__(self):
        self.config = ConfigLoader.load_config()
        self.ingestion_client = BinanceIngestionClient(self.config)
        self.transformer = DataTransformer(self.config)
        self.scheduler = BackgroundScheduler()
        self.logger = get_logger(self.__class__.__name__)
        self.raw_data_repo = RawDataRepository()
        self._configure_jobs()

    def _configure_jobs(self):
        # Schedule the data transformation job
        self.scheduler.add_job(
            self.transformer.transform_data,
            'interval',
            minutes=1,  # TODO: Run every minute
            # seconds=10,  # TODO: remove this Run every 10 seconds
            id='data_transformation'
        )
        # Schedule the raw data cleanup job
        self.scheduler.add_job(
            self._cleanup_raw_data,
            'cron',
            hour=0,  # Run daily at midnight
            id='raw_data_cleanup'
        )

    def _cleanup_raw_data(self):
        self.logger.info("Starting raw data cleanup...")
        try:
            self.raw_data_repo.delete_old_raw_data(retention_period='1 day')
            self.logger.info("Raw data cleanup completed.")
        except Exception as e:
            self.logger.error(f"Error during raw data cleanup: {e}")

    def start(self):
        self.logger.info("Starting orchestrator...")
        self.ingestion_client.start_ingestion()
        self.scheduler.start()

    def stop(self):
        self.logger.info("Stopping orchestrator...")
        self.ingestion_client.stop_ingestion()
        self.scheduler.shutdown()
        self.logger.info("Orchestrator stopped.")
