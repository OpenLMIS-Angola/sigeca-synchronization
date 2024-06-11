import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import json
from app.infrastructure import JDBCReader
from app.application import DataSyncService
from datetime import datetime
from app.infrastructure import ChangeLogOperationEnum
from app.infrastructure import ResourceAPIClient
from app.application.scheduler.data_sync_scheduler import ChangesSyncScheduler
from app.application.synchronizations import FacilityResourceSynchronization
from app.infrastructure.database import get_engine, get_session, Base
import logging


def load_config(file_path="./config.json"):
    with open(file_path, "r") as file:
        return json.load(file)


def main():
    config = load_config()

    logging.basicConfig(level=logging.INFO)

    engine = get_engine(config["changelog_database"])
    session_maker = get_session(engine)

    # Create the tables if they don't exist
    Base.metadata.create_all(engine)

    jdbc_reader = JDBCReader(config["jdbc_reader"])
    api_client = ResourceAPIClient(config["api"]["url"], config["api"]["token"])
    data_sync_service = DataSyncService(jdbc_reader, session_maker)

    sync_interval_minutes = config["sync"]["interval_minutes"]
    scheduler = ChangesSyncScheduler(
        data_sync_service,
        session_maker,
        sync_interval_minutes,
        [FacilityResourceSynchronization],
    )

    try:
        scheduler.start()
        # Keep the script running
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


if __name__ == "__main__":
    main()
