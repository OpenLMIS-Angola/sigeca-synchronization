from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional, Type

from app.domain import FacilityResourceReader
from app.domain.resources.abstract import ResourceReader
from app.infrastructure import ChangeLogOperationEnum, JDBCReader


class ResourceSynchronization(ABC):
    @property
    @abstractmethod
    def synchronized_resource(self) -> Type[ResourceReader]:
        pass

    @classmethod
    def get_resource_name(cls):
        return f"{cls.synchronized_resource.read_schema_name()}.{cls.synchronized_resource.read_table_name()}"

    def __init__(self, jdbc_reader: JDBCReader):
        self.jdbc_reader = jdbc_reader
        self.resource = self.synchronized_resource(jdbc_reader)

    def execute_full_synchronization(self):
        data = self.resource.get_all_data()
        self.synchronize_data(data)

    def execute_change_synchronization(
        self,
        operation: ChangeLogOperationEnum = None,
        last_sync_timestamp: Optional[datetime] = None,
    ):
        data = self.resource.get_changelog_data(operation, last_sync_timestamp)
        self.synchronize_data(data)

    def synchronize_data(self, df):
        data = df.toJSON().collect()
        print(data)



def resource_synchronization_factory(resource: Type[ResourceReader]): 
    x = ResourceSynchronization
    x.synchronized_resource = resource
    return x

class FacilityResourceSynchronization(ResourceSynchronization):
    synchronized_resource = FacilityResourceReader
