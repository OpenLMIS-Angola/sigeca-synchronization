import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    ProgramResourceRepository,
    FacilityTypeResourceRepository,
    BaseResourceRepository,
)
from app.infrastructure.sigeca_api_client import SigecaApiClient
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient

from app.infrastructure.jdbc_reader import JDBCReader
from .validators import validate_facilities_dataframe
import unidecode
import json
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class FacilitySupplementSynchronization(ABC):

    def __init__(
        self,
        jdbc_reader: JDBCReader,
        facilities: DataFrame,
        repo: BaseResourceRepository,
        lmis_api: OpenLmisApiClient,
    ):
        self.facilities = facilities
        self.repo = repo
        self.jdbc_reader = jdbc_reader
        self.lmis_api = lmis_api

    def synchronize(self):
        try:
            logging.info(f"Sarting synchronization for {self.endpoint}")
            self._add_missing()
            logging.info("Synchronization completed succesfully")
        except Exception as e:
            logging.error(f"An error occurred during facility synchronization: {e}")
            raise

    @abstractmethod
    def _add_missing(self):
        pass

    @property
    @abstractmethod
    def endpoint(self):
        pass

    def _sent_to_client(self, data):
        self.lmis_api.send_post_request(self.endpoint, data["payload"])
