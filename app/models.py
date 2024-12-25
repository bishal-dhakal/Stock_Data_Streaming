from sqlalchemy import Column, DateTime, Float, Integer, String

from core.database import Base


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    price = Column(Float)
    volume = Column(Integer)
    created_at = Column(DateTime)
