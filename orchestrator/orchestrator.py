from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.interval import IntervalTrigger
from ingestion.binance_ingestion import BinanceIngestionClient
from transformation.transformer import DataTransformer
from utils.config_loader import ConfigLoader
from utils.logger import get_logger
from database.raw_data_repository import RawDataRepository
from utils.state_manager import StateManager

class Orchestrator:
    def __init__(self):
        self.config = ConfigLoader.load_config()
        self.ingestion_client = BinanceIngestionClient(self.config)
        self.transformer = DataTransformer(self.config)
        executors = {
            'default': ThreadPoolExecutor(max_workers=self.config['max_workers'])
        }
        self.scheduler = BackgroundScheduler(executors=executors)
        self.logger = get_logger(self.__class__.__name__)
        self.raw_data_repo = RawDataRepository()
        self.state_manager = StateManager()
        self._configure_jobs()

    def _configure_jobs(self):
        try: 
            sampling_frequency = self.config['sampling_frequency']

            # Schedule data ingestion for each symbol
            for symbol in self.config['symbols']:
                self.scheduler.add_job(
                    self.ingestion_client.ingest_data,
                    trigger=IntervalTrigger(seconds=sampling_frequency),
                    args=[symbol],
                    id=f'ingest_data_{symbol}'
                )

            # Schedule the data transformation job
            self.scheduler.add_job(
                self.transformer.transform_data,
                'interval',
                minutes=self.config['downsampling_frequency'],
                id='data_transformation'
            )

            # Schedule the raw data cleanup job
            self.scheduler.add_job(
                self._cleanup_raw_data,
                'cron',
                hour=0,  # Run daily at midnight
                id='raw_data_cleanup'
            )
        except Exception as e:
            self.logger.error("Error configuring jobs: %s", e)

    def _cleanup_raw_data(self):
        self.logger.info("Starting raw data cleanup...")
        try:
            self.raw_data_repo.delete_all_raw_data()
            self.state_manager.reset_state()
            self.logger.info("Raw data cleanup completed.")
        except Exception as e:
            self.logger.error("Error during raw data cleanup: %s", e)

    def start(self):
        self.logger.info("Starting orchestrator...")
        self.scheduler.start()

    def stop(self):
        self.logger.info("Stopping orchestrator...")
        self.scheduler.shutdown()
        self.logger.info("Orchestrator stopped.")
