# utils/database.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.config_loader import ConfigLoader

class Database:
    _engine = None
    _SessionLocal = None

    @classmethod
    def initialize(cls):
        config = ConfigLoader.load_config()
        db_config = config['database']
        db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@" \
                 f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

        cls._engine = create_engine(db_url, pool_pre_ping=True)
        cls._SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=cls._engine)

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            cls.initialize()
        return cls._engine

    @classmethod
    def get_session(cls):
        if cls._SessionLocal is None:
            cls.initialize()
        return cls._SessionLocal()
