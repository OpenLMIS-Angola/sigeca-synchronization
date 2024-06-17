import json
import logging
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient
from app.infrastructure.sigeca_api_client import SigecaApiClient
from app.infrastructure.database import get_engine
from app.application.synchronization.facilities import FacilitySynchronizationService
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    FacilityTypeResourceRepository,
    ProgramResourceRepository,
)
from app.infrastructure.jdbc_reader import JDBCReader


def load_config(config_path):
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
        return config


# Usage Example
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
        FacilityResourceRepository(jdbc_reader),
        GeographicZoneResourceRepository(jdbc_reader),
        FacilityTypeResourceRepository(jdbc_reader),
        FacilityOperatorResourceRepository(jdbc_reader),
        ProgramResourceRepository(jdbc_reader),
    )
    try:
        # lmis_client.login()
        ##lmis_client.get_facilities()
        # print(sigeca_client.fetch_facilities())
        sync_service.synchronize_facilities()

        # Example data
        # facility_data = {
        #     "name": "HEALTH CENTER BELA VISTA",
        #     "code": "470010",
        #     "abbreviation": "HC",
        #     "category": "Health Center",
        #     "ownership": "Public - National Health Service",
        #     "management": "Public",
        #     "municipality": "Ambriz",
        #     "province": "Bengo",
        #     "operational": True,
        #     "latitude": "7.81807",
        #     "longitude": "1380299"
        # }

        # geo_zone_data = {
        #     "name": "Ambriz",
        #     "province": "Bengo"
        # }

        # # Create facility
        # facility_response = client.create_facility(facility_data)
        # print("Facility created successfully:", facility_response)

        # # Update facility
        # updated_facility_data = {
        #     "name": "HEALTH CENTER BELA VISTA - UPDATED",
        #     "abbreviation": "HC",
        #     "category": "Health Center",
        #     "ownership": "Public - National Health Service",
        #     "management": "Public",
        #     "municipality": "Ambriz",
        #     "province": "Bengo",
        #     "operational": True,
        #     "latitude": "7.81807",
        #     "longitude": "1380299"
        # }
        # update_response = client.update_facility("470010", updated_facility_data)
        # print("Facility updated successfully:", update_response)

        # # Delete facility
        # delete_response = client.delete_facility("470010")
        # print(delete_response["message"])

        # # Create geo zone
        # geo_zone_response = client.create_geo_zone(geo_zone_data)
        # print("Geographic zone created successfully:", geo_zone_response)

    except Exception as e:
        logging.exception(e)
