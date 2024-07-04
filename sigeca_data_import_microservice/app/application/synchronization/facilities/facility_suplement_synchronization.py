from app.application.synchronization.facility_types import FacilityTypeSynchronization
from app.application.synchronization.geo_zones import (
    ProvinceSynchronization,
    MunicipalitySynchronization,
)
from pyspark.sql import DataFrame

from app.application.synchronization.product import ProgramSynchronization
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityTypeResourceRepository,
    FacilityOperatorResourceRepository,
    ProgramResourceRepository,
)
from app.infrastructure.jdbc_reader import JDBCReader
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient
from app.infrastructure.sigeca_api_client import SigecaApiClient


class FacilitySupplementSync:

    def __init__(
        self,
        jdbc_reader: JDBCReader,
        lmis_client: OpenLmisApiClient,
        geo_zone_repo: GeographicZoneResourceRepository,
        facility_type_repo: FacilityTypeResourceRepository,
        program_repo: ProgramResourceRepository,
    ):
        self.lmis_client = lmis_client
        self.geo_zone_repo = geo_zone_repo
        self.facility_type_repo = facility_type_repo
        self.program_repo = program_repo
        self.jdbc_reader = jdbc_reader

    def synchronize_supplement_data(self, df):
        self.synchronize_mising_geographic_zones(df)
        self.synchronize_mising_types(df)
        self.synchronize_products(df)
        # Currently skip, on UAT no operators exist
        # self.synchronize_operators()

    def synchronize_mising_geographic_zones(self, df: DataFrame):
        ProvinceSynchronization(
            self.jdbc_reader, df, self.geo_zone_repo, self.lmis_client
        ).synchronize()

        MunicipalitySynchronization(
            self.jdbc_reader, df, self.geo_zone_repo, self.lmis_client
        ).synchronize()

    def synchronize_mising_types(self, df):
        FacilityTypeSynchronization(
            self.jdbc_reader, df, self.facility_type_repo, self.lmis_client
        ).synchronize()

    def synchronize_products(self, df):
        ProgramSynchronization(
            self.jdbc_reader, df, self.program_repo, self.lmis_client
        ).synchronize()
