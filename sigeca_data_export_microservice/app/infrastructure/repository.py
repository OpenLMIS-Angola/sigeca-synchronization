from app.domain.models.data_chages import DataChanges
from app.domain.models.sync_log import SyncLog
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc


class SyncLogRepository:
    def __init__(self, session: Session):
        self.session = session

    def add_log(
            self,
            resource: str,
            sync_type: str,
            operation: str,
            success: bool,
            message: Optional[str],
            details: Optional[str],
    ):
        log = SyncLog(
            resource=resource,
            operation=operation,
            success=success,
            message=message,
            details=details,
            sync_type=sync_type,
        )
        self.session.add(log)
        self.session.commit()

    def get_most_recent_successful_report(self, resource: str) -> Optional[SyncLog]:
        query = self.session.query(SyncLog).filter(SyncLog.success == True, SyncLog.resource == resource)
        query = query.order_by(desc(SyncLog.timestamp)).first()
        return query


class DataChangesRepository:
    def __init__(self, session: Session):
        self.session = session

    def clear_data_changes(self):
        self.session.query(DataChanges).delete()
