# utils/state_manager.py

import threading
from database.state_repository import StateRepository


class StateManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.repository = StateRepository()

    def get_collected_points(self, symbol):
        with self.lock:
            return self.repository.get_collected_points(symbol)

    def update_collected_points(self, symbol, collected_points):
        with self.lock:
            self.repository.update_collected_points(symbol, collected_points)

    def reset_state(self):
        with self.lock:
            self.repository.reset_state()
