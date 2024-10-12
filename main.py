import time
from ingestion.binance_ingestion import BinanceIngestionClient
from utils.config_loader import ConfigLoader

def main():
    config = ConfigLoader.load_config()
    ingestion_client = BinanceIngestionClient(config)

    try:
        ingestion_client.start_ingestion()
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Stopping ingestion...")
        ingestion_client.stop_ingestion()
    except Exception as e:
        print(f"An error occurred: {e}")
        ingestion_client.stop_ingestion()

if __name__ == '__main__':
    main()
