from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional, Type

from app.domain import FacilityResourceReader
from app.domain.resources.abstract import ResourceReader
from app.infrastructure import ChangeLogOperationEnum, JDBCReader
from app.infrastructure.repository import SyncLogRepository
from app.application.synchronizations.abstract import ResourceSynchronization
from app.domain.models.sync_log import SyncLog


class DataSyncService:
    def __init__(self, jdbc_reader: JDBCReader, session_maker: SyncLogRepository):
        self.jdbc_reader = jdbc_reader
        self.session_maker = session_maker

    def sync_change(
        self,
        resource: Type[ResourceSynchronization],
        operation: Optional[ChangeLogOperationEnum] = None,
        from_datetime: Optional[datetime] = None,
    ):
        resource(self.jdbc_reader).execute_change_synchronization(
            operation, from_datetime
        )

    def sync_from_last_sucessfull_synchronization(
        self,
        resource: Type[ResourceSynchronization],
        operation: Optional[ChangeLogOperationEnum] = None,
    ):
        last_sync = self._get_last_successful_sync(
            resource.get_resource_name(), operation
        )
        self.sync_change(resource, operation, last_sync.timestamp)

    def sync_full(self, resource: Type[ResourceSynchronization]):
        resource(self.jdbc_reader).execute_full_synchronization()

    def _get_last_successful_sync(
        self,
        resource_name: Optional[str] = None,
        operation: Optional[ChangeLogOperationEnum] = None,
    ) -> SyncLog:
        session = self.session_maker()
        sync_log_repo = SyncLogRepository(session)

        try:
            last = sync_log_repo.get_most_recent_successful_report(
                resource_name, operation.value if operation else None
            )
            return last
        finally:
            session.close()
