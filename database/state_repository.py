# database/state_repository.py

from .models import IngestionState
from .database import Database
from sqlalchemy.orm import Session
import logging

class StateRepository:
    def __init__(self):
        self.engine = Database.get_engine()
        Base = IngestionState.__base__
        Base.metadata.create_all(self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)

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
                self.logger.debug(f"Incremented collected points for {symbol}: {state.collected_points}")
            else:
                state = IngestionState(symbol=symbol, collected_points=1)  # Initialize to 1
                session.add(state)
                self.logger.debug(f"Created new IngestionState for {symbol} with collected_points=1")
            session.commit()
            self.logger.info(f"Updated collected points for {symbol}")
            self.logger.info(f"Collected points: {state.collected_points}")
            return state.collected_points
        except Exception as e:
            self.logger.error(f"Error updating collected points for {symbol}: {e}")
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
            self.logger.error(f"Error resetting ingestion states: {e}")
            session.rollback()
            raise e
        finally:
            session.close()
