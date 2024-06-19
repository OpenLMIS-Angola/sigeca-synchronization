import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import argparse
import json
import logging
from datetime import datetime

from app.application import DataSyncService
from app.application.scheduler.sigeca_data_export_scheduler import ChangesSyncScheduler
from app.application.synchronizations import FacilityResourceSynchronization
from app.infrastructure import (ChangeLogOperationEnum, JDBCReader,
                                ResourceAPIClient)
from app.infrastructure.database import Base, get_engine, get_session


def load_config(file_path="./config.json"):
    with open(file_path, "r") as file:
        return json.load(file)


def _run_scheduler(session_maker, jdbc_reader, sigeca_data_export_service, sync_interval_minutes):
    try:
        scheduler = ChangesSyncScheduler(
            sigeca_data_export_service,
            session_maker,
            sync_interval_minutes,
            [FacilityResourceSynchronization(jdbc_reader)],
        )

        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()

def main():
    config = load_config()

    logging.basicConfig(level=logging.INFO)

    engine = get_engine(config["changelog_database"])
    session_maker = get_session(engine)

    # Create the tables if they don't exist
    Base.metadata.create_all(engine)

    jdbc_reader = JDBCReader(config["jdbc_reader"])
    api_client = ResourceAPIClient(config["api"]["url"], config["api"]["token"])
    sigeca_data_export_service = DataSyncService(session_maker)

    sync_interval_minutes = config["sync"]["interval_minutes"]

    parser = argparse.ArgumentParser(description="Data synchronization service")
    parser.add_argument("--run-mode", choices=["continuous", "one-time"], required=True, help="Run mode: 'continuous' to start the scheduler or 'one-time' to execute one-time integration")
    args = parser.parse_args()

    if args.run_mode == "continuous":
        _run_scheduler(session_maker, jdbc_reader, sigeca_data_export_service, sync_interval_minutes)

    elif args.run_mode == "one-time":
        sigeca_data_export_service.sync_full(
            FacilityResourceSynchronization(jdbc_reader)
        )


if __name__ == "__main__":
    main()
