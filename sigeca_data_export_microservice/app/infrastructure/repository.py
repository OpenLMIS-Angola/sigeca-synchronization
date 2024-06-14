from sqlalchemy.orm import Session
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
        message: str,
        details: str = None,
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

    def get_most_recent_successful_report(
        self, resource: Optional[str] = None, operation: Optional[str] = None
    ) -> Optional[SyncLog]:
        query = self.session.query(SyncLog).filter(SyncLog.success == True)

        if resource:
            query = query.filter(SyncLog.resource == resource)

        if operation:
            query = query.filter(SyncLog.operation == operation)

        query = query.order_by(desc(SyncLog.timestamp)).first()
        return query
