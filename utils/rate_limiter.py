import threading
import time

class RateLimiter:
    def __init__(self, max_calls_per_second):
        self.capacity = max_calls_per_second
        self.tokens = self.capacity
        self.fill_rate = self.capacity
        self.lock = threading.Lock()
        self.last_check = time.monotonic()

    def __enter__(self):
        with self.lock:
            current = time.monotonic()
            elapsed = current - self.last_check
            self.last_check = current
            self.tokens += elapsed * self.fill_rate
            if self.tokens > self.capacity:
                self.tokens = self.capacity
            if self.tokens < 1:
                time_to_wait = (1 - self.tokens) / self.fill_rate
                time.sleep(time_to_wait)
                self.tokens = 0
            else:
                self.tokens -= 1

    def __exit__(self, exc_type, exc_value, traceback):
        pass
