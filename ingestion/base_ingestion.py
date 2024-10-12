from abc import ABC, abstractmethod

class DataIngestionClient(ABC):
    @abstractmethod
    def start_ingestion(self):
        """Start the data ingestion process."""
        pass

    @abstractmethod
    def stop_ingestion(self):
        """Stop the data ingestion process."""
        pass

    @abstractmethod
    def ingest_data(self):
        """Ingest data for a single symbol."""
        pass
