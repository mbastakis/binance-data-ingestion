from sqlalchemy import Column, String, TIMESTAMP, Float, JSON, PrimaryKeyConstraint, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RawData(Base):
    __tablename__ = 'raw_data'
    symbol = Column(String, nullable=False)
    data = Column(JSON)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'timestamp'),
    )

class DownsampledData(Base):
    __tablename__ = 'downsampled_data'
    symbol = Column(String, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    avg_price = Column(Float)
    median_price = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'timestamp'),
    )

class IngestionState(Base):
    __tablename__ = 'ingestion_state'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, unique=True, nullable=False)
    collected_points = Column(Integer, nullable=False, default=0)