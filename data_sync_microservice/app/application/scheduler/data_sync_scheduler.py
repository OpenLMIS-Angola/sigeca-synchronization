from apscheduler.schedulers.background import BackgroundScheduler
from app.application.services.data_sync_service import DataSyncService
from app.infrastructure.api import ResourceAPIClient
from app.infrastructure.utils import ChangeLogOperationEnum
from sqlalchemy.orm import sessionmaker
from app.infrastructure.repository import SyncLogRepository
from datetime import datetime
from app.application.synchronizations import FacilityResourceSynchronization
from app.domain import FacilityResourceReader
import logging


class ChangesSyncScheduler:
    def __init__(
        self,
        sync_service: DataSyncService,
        session_maker: sessionmaker,
        sync_interval_minutes: int,
    ):
        self.sync_service = sync_service
        self.scheduler = BackgroundScheduler()
        self.session_maker = session_maker
        self.sync_interval_minutes = sync_interval_minutes

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

        operation = ChangeLogOperationEnum.UPDATE
        resource = FacilityResourceSynchronization

        resource_name = resource.get_resource_name()

        # self.sync_service.sync_full(FacilityResourceSynchronization)
        try:
            self.sync_service.sync_from_last_sucessfull_synchronization(
                resource, operation
            )
            sync_log_repo.add_log(
                resource_name,
                "CHANGE",
                operation.value,
                True,
                "Synchronization succeeded",
            )
            logging.info(
                f"Synchronization succeeded for resource: {resource}, operation: {operation.value}"
            )
        except Exception as e:
            import traceback
            sync_log_repo.add_log(
                resource_name,
                "CHANGE",
                operation.value,
                False,
                "Synchronization failed",
                str(e),
            )
            logging.exception(f"Synchronization failed for resource: {resource}, operation: {operation.value}. Error: {e}")
        finally:
            session.close()
