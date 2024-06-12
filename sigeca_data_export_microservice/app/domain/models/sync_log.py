from sqlalchemy import Column, Integer, String, DateTime, Boolean
from datetime import datetime
from app.infrastructure.database import Base


class SyncLog(Base):
    __tablename__ = "sync_logs"
    __table_args__ = {"schema": "changelog"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    resource = Column(String, nullable=False)
    sync_type = Column(String, nullable=True)
    operation = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    success = Column(Boolean, nullable=False)
    message = Column(String, nullable=True)
    details = Column(String, nullable=True)
