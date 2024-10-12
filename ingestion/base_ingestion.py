from abc import ABC, abstractmethod

class DataIngestionClient(ABC):
    @abstractmethod
    def ingest_data(self):
        """Ingest data for a single symbol."""
