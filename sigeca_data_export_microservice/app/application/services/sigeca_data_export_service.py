from sqlalchemy.orm import sessionmaker

from app.infrastructure import ChangeLogOperation
from app.infrastructure.repository import SyncLogRepository, DataChangesRepository
from app.application.synchronizations.abstract import ResourceSynchronization
from app.domain.models.sync_log import SyncLog


class DataSyncService:
    def __init__(self, session_maker: sessionmaker):
        self.session_maker = session_maker

    def sync_log(self, resource: ResourceSynchronization):
        last_sync = self._get_last_successful_sync(resource.get_resource_name())
        if last_sync:
            self._sync_change(resource)
        else:
            self.sync_full(resource)

    def sync_full(self, resource: ResourceSynchronization):
        try:
            resource.execute_full_synchronization()
            self._save_successful_sync(resource.get_resource_name(), "full", True)
        except Exception as e:
            self._save_successful_sync(resource.get_resource_name(), "full", False)
            raise e

    def clear_logs(self):
        session = self.session_maker()
        data_changes_repo = DataChangesRepository(session)

        try:
            data_changes_repo.clear_data_changes()
            session.commit()
        finally:
            session.close()

    def _sync_change(self, resource: ResourceSynchronization):
        try:
            resource.execute_change_synchronization()
            self._save_successful_sync(resource.get_resource_name(), "change", True)
        except Exception as e:
            self._save_successful_sync(resource.get_resource_name(), "change", False)
            raise e

    def _get_last_successful_sync(self, resource_name: str) -> SyncLog:
        session = self.session_maker()
        sync_log_repo = SyncLogRepository(session)

        try:
            last = sync_log_repo.get_most_recent_successful_report(resource_name)
            return last
        finally:
            session.close()

    def _save_successful_sync(self, resource_name: str, sync_type: str, success: bool):
        session = self.session_maker()
        sync_log_repo = SyncLogRepository(session)

        try:
            sync_log_repo.add_log(
                resource_name,
                sync_type=sync_type,
                operation=ChangeLogOperation.SYNC.value,
                success=success,
                message=None,
                details=None,
            )
            session.commit()
        finally:
            session.close()
