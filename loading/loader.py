# loading/loader.py

from database.raw_data_repository import RawDataRepository


class DataLoader:
    def __init__(self, config):
        self.repository = RawDataRepository()

    def load_raw_data(self, symbol, data, timestamp):
        self.repository.insert_raw_data(symbol, data, timestamp)
