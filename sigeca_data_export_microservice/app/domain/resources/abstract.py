import json
import pprint
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Type

import requests
from app.infrastructure import ChangeLogOperationEnum, JDBCReader
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, expr, lit
from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StringType
from pyspark.sql.types import StringType as UUIDType
from pyspark.sql.types import StructField, StructType
from datetime import datetime


class ResourceReader(ABC):
    def __init__(self, jdbc_reader: JDBCReader):
        self.jdbc_reader = jdbc_reader

    @classmethod
    @abstractmethod
    def read_schema_name(cls) -> StructType:
        pass

    @classmethod
    @abstractmethod
    def read_table_name(cls) -> StructType:
        pass

    @abstractmethod
    def read_schema(self) -> StructType:
        pass

    @abstractmethod
    def transform_data(self, df: DataFrame) -> DataFrame:
        pass

    def get_all_data(self) -> DataFrame:
        format_name = f"{self.read_schema_name()}.{self.read_table_name()}"
        data = self.jdbc_reader.read_data(format_name, self.read_schema())
        transformed_data = self.transform_data(data)
        return transformed_data

    def get_changelog_data(
        self,
        operation: ChangeLogOperationEnum = None,
        last_sync_timestamp: Optional[datetime] = None,
    ) -> DataFrame:
        format_name = f"{self.read_schema_name()}.{self.read_table_name()}"
        data = self.jdbc_reader.read_changes(
            format_name, self.read_schema(), operation, last_sync_timestamp
        )
        transformed_data = self.transform_data(data)
        return transformed_data
