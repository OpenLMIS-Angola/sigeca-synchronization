from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from app.application.services.sigeca_data_export_service import DataSyncService
from app.infrastructure.api import ResourceAPIClient
from app.infrastructure.utils import ChangeLogOperationEnum
from sqlalchemy.orm import sessionmaker
from app.infrastructure.repository import SyncLogRepository
from datetime import datetime
from app.application.synchronizations import FacilityResourceSynchronization
from app.application.synchronizations.abstract import ResourceSynchronization
from app.domain import FacilityResourceReader
import logging


class ChangesSyncScheduler:
    def __init__(
        self,
        sync_service: DataSyncService,
        session_maker: sessionmaker,
        sync_interval_minutes: int,
        synchronized_resources: List[ResourceSynchronization],
        synchronized_operations: List[ChangeLogOperationEnum] = None,
    ):
        self.sync_service = sync_service
        self.scheduler = BackgroundScheduler()
        self.session_maker = session_maker
        self.sync_interval_minutes = sync_interval_minutes
        self.synchronized_resources = synchronized_resources
        self.synchronized_operations = synchronized_operations

    def start(self):
        self.scheduler.add_job(
            self.run_sync, "interval", minutes=self.sync_interval_minutes
        )
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()

    def run_sync(self):
        session = self.session_maker()
        sync_log_repo = SyncLogRepository(session)

        try:
            for resource in self.synchronized_resources:
                if not self.synchronized_operations:
                    self._synchronize_resource(sync_log_repo, resource)
                else:
                    for operation in self.synchronized_operations:
                        self._synchronize_resource(sync_log_repo, resource, operation)
        except Exception as e:
            logging.exception(f"Synchronization job failed. Error: {e}")
        finally:
            session.close()

    def _synchronize_resource(self, sync_log_repo, resource, operation=None):
        operation_value = operation.value if operation else "ALL"
        try:
            resource_name = resource.get_resource_name()
            self.sync_service.sync_from_last_sucessfull_synchronization(
                resource, operation
            )
            sync_log_repo.add_log(
                resource_name,
                "CHANGE",
                operation_value,
                True,
                "Synchronization succeeded",
            )
            logging.info(
                f"Synchronization succeeded for resource: {resource}, operation: {operation_value}"
            )
        except Exception as e:
            sync_log_repo.add_log(
                resource_name,
                "CHANGE",
                operation_value,
                False,
                "Synchronization failed",
                str(e),
            )
            logging.exception(
                f"Synchronization failed for resource: {resource}, operation: {operation_value}. Error: {e}"
            )
