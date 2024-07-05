import argparse
import json
import logging
import os

from app.application.scheduler import FacilitySyncScheduler
from app.application.synchronization.facilities.synchronization import (
    FacilitySynchronizationService,
)
from app.domain.resources import (
    FacilityOperatorResourceRepository,
    FacilityResourceRepository,
    FacilityTypeResourceRepository,
    GeographicZoneResourceRepository,
    ProgramResourceRepository,
)
from app.infrastructure.database import get_engine
from app.infrastructure.jdbc_reader import JDBCReader
from app.infrastructure.smtp_client import SMTPClient
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient
from app.infrastructure.sigeca_api_client import SigecaApiClient
from dotenv import load_dotenv
from app.config import Config


def load_config(from_env=False):
    if from_env:
        load_dotenv("./settings.env")
        config = os.getenv("sigeca_import_config")
        if not config:
            raise KeyError("Provided settings.env missing `sigeca_import_config` key.")
        return json.loads(config)
    else:
        with open("config.json", "r") as config_file:
            config = json.load(config_file)
            return config


def _run_scheduler(sync_service, sync_interval_minutes):
    try:
        scheduler = FacilitySyncScheduler(sync_service, sync_interval_minutes)

        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data synchronization service")
    parser.add_argument(
        "--run-mode",
        choices=["continuous", "one-time"],
        required=True,
        help="Run mode: 'continuous' to start the scheduler or 'one-time' to execute one-time integration",
    )
    parser.add_argument(
        "--env-config",
        required=False,
        action="store_true",
        help="Env Config: use stringified config comming form env instead of .json file",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    config = load_config(args.env_config)
    config = Config.from_dict(config)

    engine = get_engine(Config().database)

    lmis_client = OpenLmisApiClient(config.open_lmis_api)
    sigeca_client = SigecaApiClient(config.sigeca_api)
    jdbc_reader = JDBCReader(config.jdbc_reader)
    smtp_client = SMTPClient(config.smtp)

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
        if config.jdbc_reader.ssh_user:
            jdbc_reader.setup_ssh_tunnel()

        if args.run_mode == "continuous":
            sync_interval_minutes = config["sync"]["interval_minutes"]
            _run_scheduler(sync_service, sync_interval_minutes)

        elif args.run_mode == "one-time":
            lmis_client.login()
            sync_service.synchronize_facilities()
    except Exception as e:
        logging.exception(e)
    finally:
        if config.jdbc_reader.ssh_user:
            jdbc_reader.close_ssh_tunnel()
