from apscheduler.schedulers.background import BackgroundScheduler
from ingestion.binance_ingestion import BinanceIngestionClient
from transformation.transformer import DataTransformer
from utils.config_loader import ConfigLoader
from utils.logger import get_logger
import psycopg2

class Orchestrator:
    def __init__(self):
        self.config = ConfigLoader.load_config()
        self.ingestion_client = BinanceIngestionClient(self.config)
        self.transformer = DataTransformer(self.config)
        self.scheduler = BackgroundScheduler()
        self.logger = get_logger(self.__class__.__name__)
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
            db_config = self.config['database']
            connection = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                dbname=db_config['dbname']
            )
            with connection.cursor() as cursor:
                cursor.execute('''
                    DELETE FROM raw_data
                    WHERE timestamp < NOW() - INTERVAL '1 day'
                ''')
                connection.commit()
            self.logger.info("Raw data cleanup completed.")
        except Exception as e:
            self.logger.error(f"Error during raw data cleanup: {e}")
        finally:
            if connection:
                connection.close()

    def start(self):
        self.logger.info("Starting orchestrator...")
        self.ingestion_client.start_ingestion()
        self.scheduler.start()

    def stop(self):
        self.logger.info("Stopping orchestrator...")
        self.ingestion_client.stop_ingestion()
        self.scheduler.shutdown()
        self.logger.info("Orchestrator stopped.")
