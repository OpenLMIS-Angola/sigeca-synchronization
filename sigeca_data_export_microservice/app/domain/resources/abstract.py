from abc import ABC, abstractmethod
from typing import Optional

from app.infrastructure import ChangeLogOperation, JDBCReader
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from datetime import datetime


class ResourceReader(ABC):
    def __init__(self, jdbc_reader: JDBCReader):
        self.jdbc_reader = jdbc_reader

    @classmethod
    @abstractmethod
    def read_schema_name(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def read_table_name(cls) -> str:
        pass

    @abstractmethod
    def read_schema(self) -> StructType:
        pass

    @abstractmethod
    def transform_data(self, df: DataFrame) -> list[dict]:
        pass

    def get_all_data(self) -> list[dict]:
        format_name = f"{self.read_schema_name()}.{self.read_table_name()}"
        data = self.jdbc_reader.read_data(format_name, self.read_schema())
        transformed_data = self.transform_data(data)
        return transformed_data

    def get_changelog_data(
            self,
            operation: ChangeLogOperation = None,
            last_sync_timestamp: Optional[datetime] = None,
    ) -> list[dict]:
        format_name = f"{self.read_schema_name()}.{self.read_table_name()}"
        data = self.jdbc_reader.read_changes(
            format_name, self.read_schema(), operation, last_sync_timestamp
        )
        transformed_data = self.transform_data(data)
        return transformed_data
