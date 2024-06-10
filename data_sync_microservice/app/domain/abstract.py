from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, StringType as UUIDType
from pyspark.sql.functions import col, lit, current_date, expr
import requests
import json
import pprint
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Callable, Any

from app.infrastructure import JDBCReader

class ResourceReader(ABC):
    def __init__(self, jdbc_reader: JDBCReader):
        self.jdbc_reader = jdbc_reader

    @abstractmethod
    def read_schema_name(self) -> StructType:
        pass

    @abstractmethod
    def read_table_name(self) -> StructType:
        pass

    @abstractmethod
    def read_schema(self) -> StructType:
        pass

    @abstractmethod
    def transform_data(self, df: DataFrame) -> DataFrame:
        pass

    def get_all_data(self) -> DataFrame:
        format_name = F"{self.read_schema_name()}.{self.read_table_name()}"
        data = self.jdbc_reader.read_data(format_name, self.read_schema())
        transformed_data = self.transform_data(data)
        return transformed_data

    def get_changelog_data(self) -> DataFrame:
        format_name = F"{self.read_schema_name()}.{self.read_table_name()}"
        data = self.jdbc_reader.read_changes(format_name, self.read_schema())
        transformed_data = self.transform_data(data)
        return transformed_data