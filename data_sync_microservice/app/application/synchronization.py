from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, StringType as UUIDType
from pyspark.sql.functions import col, lit, current_date, expr
import requests
import json
import pprint
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Callable, Any, Type

from app.infrastructure import JDBCReader
from app.domain.abstract import ResourceReader
from app.domain import FacilityResourceReader

class ResourceSynchronization(ABC):
    @property
    @abstractmethod
    def synchronized_resource(self) -> Type[ResourceReader]:
        pass
     
    def __init__(self, jdbc_reader: JDBCReader): 
        self.jdbc_reader = jdbc_reader
        self.resource = self.synchronized_resource(jdbc_reader)

    def execute(self):
        data = self.resource.get_data()
        self.synchronize_data(data)

    def synchronize_data(self, df):
        data = df.toJSON().collect()
        print(data)

class FacilityResourceSynchronization(ResourceSynchronization): 
    synchronized_resource = FacilityResourceReader
