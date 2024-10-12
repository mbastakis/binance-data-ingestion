from .models import IngestionState
from .database import Database
from utils.logger import get_logger

class StateRepository:
    def __init__(self):
        self.engine = Database.get_engine()
        Base = IngestionState.__base__
        Base.metadata.create_all(self.engine)
        self.logger = get_logger(self.__class__.__name__)

    def get_collected_points(self, symbol):
        session = Database.get_session()
        try:
            state = session.query(IngestionState).filter_by(symbol=symbol).first()
            return state.collected_points if state else 0
        finally:
            session.close()

    def update_collected_points(self, symbol):
        session = Database.get_session()
        try:
            state = session.query(IngestionState).filter_by(symbol=symbol).first()
            if state:
                state.collected_points += 1
                self.logger.debug("Incremented collected points for %s: %d", symbol, state.collected_points)
            else:
                state = IngestionState(symbol=symbol, collected_points=1)  # Initialize to 1
                session.add(state)
                self.logger.debug("Created new IngestionState for %s with collected_points=1", symbol)
            session.commit()
            self.logger.info("Updated collected points for %s", symbol)
            self.logger.info("Collected points: %d", state.collected_points)
            return state.collected_points
        except Exception as e:
            self.logger.error("Error updating collected points for %s: %s", symbol, e)
            session.rollback()
            raise e
        finally:
            session.close()

    def reset_state(self):
        session = Database.get_session()
        try:
            session.query(IngestionState).delete()
            session.commit()
            self.logger.info("Reset all ingestion states.")
        except Exception as e:
            self.logger.error("Error resetting ingestion states: %s", e)
            session.rollback()
            raise e
        finally:
            session.close()
