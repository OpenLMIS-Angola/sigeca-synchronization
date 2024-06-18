import json
import logging
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient
from app.infrastructure.sigeca_api_client import SigecaApiClient
from app.infrastructure.database import get_engine
from app.application.synchronization.facilities import FacilitySynchronizationService
from app.application.scheduler import FacilitySyncScheduler
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    FacilityTypeResourceRepository,
    ProgramResourceRepository,
)
from app.infrastructure.jdbc_reader import JDBCReader
import argparse

def load_config(config_path):
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
        return config


def _run_scheduler(sync_service, sync_interval_minutes):
    try:
        scheduler = FacilitySyncScheduler(
            sync_service,
            sync_interval_minutes
        )

        scheduler.start()
        # Keep the script running
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = load_config("./config.json")
    engine = get_engine(config["database"])

    lmis_client = OpenLmisApiClient(config["open_lmis_api"])
    sigeca_client = SigecaApiClient(config["sigeca_api"])
    jdbc_reader = JDBCReader(config["jdbc_reader"])

    sync_service = FacilitySynchronizationService(
        jdbc_reader,
        sigeca_client,
        lmis_client,
        FacilityResourceRepository(jdbc_reader),
        GeographicZoneResourceRepository(jdbc_reader),
        FacilityTypeResourceRepository(jdbc_reader),
        FacilityOperatorResourceRepository(jdbc_reader),
        ProgramResourceRepository(jdbc_reader),
    )
    try:
        if config["jdbc_reader"]["ssh_user"]:
            jdbc_reader.setup_ssh_tunnel()
        lmis_client.login()

        parser = argparse.ArgumentParser(description="Data synchronization service")
        parser.add_argument("--run-mode", choices=["continuous", "one-time"], required=True, help="Run mode: 'continuous' to start the scheduler or 'one-time' to execute one-time integration")
        args = parser.parse_args()

        if args.run_mode == "continuous":
            sync_interval_minutes = config["sync"]["interval_minutes"]
            _run_scheduler(sync_service, sync_interval_minutes)

        elif args.run_mode == "one-time":
            sync_service.synchronize_facilities()

    except Exception as e:
        logging.exception(e)
    finally:
        if config["jdbc_reader"]["ssh_user"]:
            jdbc_reader.close_ssh_tunnel()
