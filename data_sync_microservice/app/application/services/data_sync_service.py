from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional, Type

from app.domain import FacilityResourceReader
from app.domain.resources.abstract import ResourceReader
from app.infrastructure import (ChangeLogOperationEnum, JDBCReader,
                                ResourceAPIClient)
from app.application.synchronizations.synchronization import ResourceSynchronization


class DataSyncService:
    def __init__(self, jdbc_reader: JDBCReader, api_client: ResourceAPIClient):
        self.jdbc_reader = jdbc_reader
        self.api_client = api_client

    def sync_change(self, resource: Type[ResourceSynchronization], operation: ChangeLogOperationEnum, from_datetime: datetime):
        resource(self.jdbc_reader).execute_change_synchronization(operation, from_datetime)

    def sync_full(self, resource: Type[ResourceSynchronization]):
        resource(self.jdbc_reader).execute_full_synchronization()
