from sqlalchemy import Column, Integer, String, DateTime, JSON
from datetime import datetime
from app.infrastructure.database import Base


class DataChanges(Base):
    __tablename__ = "data_changes"
    __table_args__ = {"schema": "changelog"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    schema_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    operation = Column(String, nullable=False)
    change_timestamp = Column(DateTime, default=lambda: datetime.now(), nullable=False)
    row_data = Column(JSON, nullable=False)
