from .models import IngestionState
from .database import Database
from sqlalchemy.orm import Session

class StateRepository:
    def __init__(self):
        self.engine = Database.get_engine()
        Base = IngestionState.__base__
        Base.metadata.create_all(self.engine)

    def get_collected_points(self, symbol):
        session = Database.get_session()
        try:
            state = session.query(IngestionState).filter_by(symbol=symbol).first()
            return state.collected_points if state else 0
        finally:
            session.close()

    def update_collected_points(self, symbol, collected_points):
        session = Database.get_session()
        try:
            state = session.query(IngestionState).filter_by(symbol=symbol).first()
            if state:
                state.collected_points = collected_points
            else:
                state = IngestionState(symbol=symbol, collected_points=collected_points)
                session.add(state)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def reset_state(self):
        session = Database.get_session()
        try:
            session.query(IngestionState).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
