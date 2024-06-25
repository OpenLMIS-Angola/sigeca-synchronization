import os
import sys
import argparse
import json
import logging

from app.application.synchronizations.resources_synchronization import ChangeLogResourceSynchronization, \
    get_full_sync_list
from app.application import DataSyncService
from app.application.scheduler.sigeca_data_export_scheduler import ChangesSyncScheduler
from app.infrastructure import JDBCReader, SigecaApiClient
from app.infrastructure.database import Base, get_engine, get_session

sys.path.append(os.path.abspath(os.path.dirname(__file__)))


def _load_config(file_path="./config.json"):
    with open(file_path, "r") as file:
        return json.load(file)


def _run_scheduler(jdbc_reader, api_client, sigeca_data_export_service, sync_config):
    try:
        scheduler = ChangesSyncScheduler(
            sigeca_data_export_service,
            sync_config,
            jdbc_reader,
            api_client,
        )
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


def main():
    config = _load_config()

    logging.basicConfig(level=logging.INFO)

    engine = get_engine(config["changelog_database"])
    session_maker = get_session(engine)

    # Create the tables if they don't exist
    Base.metadata.create_all(engine)

    jdbc_reader = JDBCReader(config["jdbc_reader"])
    api_client = SigecaApiClient(config["sigeca_api"])
    sigeca_data_export_service = DataSyncService(session_maker)

    sync_config = config.get("sync", {})

    parser = argparse.ArgumentParser(description="Data synchronization service")
    parser.add_argument("--run-mode", choices=["continuous", "one-time"], required=True,
                        help="Run mode: 'continuous' to start the scheduler or 'one-time' to execute one-time integration")
    args = parser.parse_args()

    if args.run_mode == "continuous":
        _run_scheduler(jdbc_reader, api_client, sigeca_data_export_service, sync_config)

    elif args.run_mode == "one-time":
        resources = get_full_sync_list(jdbc_reader, api_client)
        for resource in resources:
            sigeca_data_export_service.sync_full(resource)


if __name__ == "__main__":
    main()
