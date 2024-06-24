from .synchronization.facilities import FacilitySynchronizationService
import logging
from apscheduler.schedulers.background import BlockingScheduler


class FacilitySyncScheduler:
    def __init__(
        self,
        sync_service: FacilitySynchronizationService,
        interval: int
    ):
        self.sync_service = sync_service
        self.sync_interval_minutes = interval
        self.scheduler = BlockingScheduler()

    def start(self):
        self.run_sync() # Sync immedietly don't wait the interval for first trigger 
        self.scheduler.add_job(
            self.run_sync, "interval", minutes=self.sync_interval_minutes
        )
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()

    def run_sync(self):
        try:
            self.sync_service.lmis_client.login()
            self.sync_service.synchronize_facilities()
        except Exception as e:
            logging.exception(f"Synchronization job failed. Error: {e}")
