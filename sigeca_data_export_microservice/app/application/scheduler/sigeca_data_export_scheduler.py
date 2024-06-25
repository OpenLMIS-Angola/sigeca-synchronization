from threading import Lock

from apscheduler.schedulers.background import BlockingScheduler
from app.application.services.sigeca_data_export_service import DataSyncService
from app.application.synchronizations.resources_synchronization import ChangeLogResourceSynchronization, \
    get_full_sync_list
from app.infrastructure import JDBCReader, SigecaApiClient

import logging

default_changelog_sync_cron = {
    "minute": "*/5"
}

default_full_sync_cron = {
    "hour": "1"
}


class ChangesSyncScheduler:
    def __init__(
            self,
            sync_service: DataSyncService,
            config: dict,
            jdbc_reader: JDBCReader,
            api_client: SigecaApiClient,
    ):
        self.sync_service = sync_service
        self.scheduler = BlockingScheduler()
        self.config = config
        self.jdbc_reader = jdbc_reader
        self.api_client = api_client
        self._sync_lock = Lock()

    def start(self):
        logging.info("Starting scheduler")
        self.scheduler.add_job(
            self._sync_changelog, "cron", **self.config.get("changelog_sync_cron", default_changelog_sync_cron)
        )
        self.scheduler.add_job(
            self._sync_full, "cron", **self.config.get("full_sync_cron", default_full_sync_cron)
        )
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()

    def _sync_changelog(self):
        with self._sync_lock:
            try:
                self.sync_service.sync_log(
                    ChangeLogResourceSynchronization(self.jdbc_reader, self.api_client))
                logging.info(f"Changelog synchronization job succeeded")
            except Exception as e:
                logging.exception(f"Changelog synchronization job failed. Error: {e}")

    def _sync_full(self):
        with self._sync_lock:
            self._full_sync_running = True
            resources = get_full_sync_list(self.jdbc_reader, self.api_client)
            try:
                for resource in resources:
                    self.sync_service.sync_full(resource)
                self.sync_service.clear_logs()
                logging.info(f"Full synchronization job succeeded")
            except Exception as e:
                logging.exception(f"Full synchronization job failed. Error: {e}")
