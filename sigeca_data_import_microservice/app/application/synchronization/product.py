import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, upper, explode
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    ProgramResourceRepository,
    FacilityTypeResourceRepository,
)
from app.infrastructure.sigeca_api_client import SigecaApiClient
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient

from app.infrastructure.jdbc_reader import JDBCReader
from .validators import validate_facilities_dataframe
import unidecode
import json
from .abstract import FacilitySupplementSynchronization

logger = logging.getLogger(__name__)


class ProgramSynchronization(FacilitySupplementSynchronization):
    endpoint = "programs"

    def _create_joined_df(self):
        df = self.facilities.alias("facilities").withColumn(
            "supported_program", explode(col("services"))
        )
        programs = self.repo.get_all().alias("program")
        df = df.join(programs, df["supported_program.code"] == programs["code"], "left")

        return df

    def validate(self, facilities_df):
        # Extract services names from the nested structure
        missing = self._validate_missing(facilities_df)
        return missing

    def _validate_missing(self, facilities_df):
        missing = facilities_df.filter((col(f"program.id").isNull()))

        num_invalid = missing.count()
        if num_invalid > 0:
            logger.warning(f"Found {num_invalid} non existing program for facilities")
            # Log details of invalid entries
        else:
            logger.info(f"All productss matching.")
        return missing

    def _add_missing(self):
        df = self._create_joined_df()
        missing: DataFrame = self.validate(df)

        reduced_df = missing[
            ["supported_program.name", "supported_program.code"]
        ].distinct()

        format_payload_f = udf(
            lambda name, code: json.dumps(
                {"code": code, "name": name, "description": name, active: True}
            )
        )
        reduced_df = reduced_df.withColumn(
            "payload",
            format_payload_f(
                col(f"name"),
                col("code"),
            ),
        )

        reduced_df.show()
        for row in reduced_df.collect():
            self._sent_to_client(row)
