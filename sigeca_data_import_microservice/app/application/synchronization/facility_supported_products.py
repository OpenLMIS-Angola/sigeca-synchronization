import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, upper
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    ProgramResourceRepository,
    FacilityTypeResourceRepository,
)
from app.infrastructure.sigeca_api_client import SigecaApiClient

from app.infrastructure.jdbc_reader import JDBCReader
from .validators import validate_facilities_dataframe
import unidecode
import json

logger = logging.getLogger(__name__)


class FacilitySupportedProductsSynchronization:
    def __init__(
        self,
        jdbc_reader: JDBCReader,
        facilities: DataFrame,
        repo: ProgramResourceRepository,
    ):
        self.facilities = facilities
        self.repo = repo
        self.jdbc_reader = jdbc_reader

    def synchronize(self):
        try:
            self._add_missing_facility_types()
            logging.info("Facility products synchronization completed successfully")
        except Exception as e:
            logging.error(
                f"An error occurred during facility products synchronization: {e}"
            )
            raise

    def _create_joined_df(self):
        df = self.facilities.alias("facilities")
        types = self.repo.get_all().alias("facility_type")
        # Validate foreign keys
        df = df.join(types, df["category"] == types["name"], "left")

        return df

    def validate(self, facilities_df):
        # Extract services names from the nested structure
        missing = self._validate_missing(facilities_df)
        return missing

    def _validate_missing(self, facilities_df):
        missing = facilities_df.filter((col(f"facility_type.id").isNull()))

        num_invalid = missing.count()
        if num_invalid > 0:
            logger.warning(
                f"Found {num_invalid} facilities with non existing type present:"
            )
            # Log details of invalid entries
            missing.distinct()[
                ["facilities.code", "facilities.name", "category"]
            ].show()
        else:
            logger.info(f"All facility products matching.")
        return missing

    def _add_missing_facility_types(self):
        df = self._create_joined_df()
        missing: DataFrame = self.validate(df)

        reduced_df = missing[["category"]].distinct()
        min_display_order = (
            self.repo.get_all()
            .alias("facility_type")
            .sort(col("displayorder").desc())
            .first()["displayorder"]
        )
        display_orders = [
            min_display_order + n for n in range(1, reduced_df.count() + 1)
        ]
        add_display_order_f = udf(lambda: display_orders.pop())
        add_category_code_f = udf(lambda name: f"{unidecode.unidecode(name.lower())}")
        format_payload_f = udf(
            lambda name, code, display: json.dumps(
                {"code": code, "name": name, "displayOrder": display}
            )
        )
        reduced_df = (
            reduced_df.withColumn("display", add_display_order_f())
            .withColumn("code", add_category_code_f(col("category")))
            .withColumn(
                "payload",
                format_payload_f(
                    col(f"category"),
                    col("code"),
                    col("display"),
                ),
            )
        )

        reduced_df.show()
        for row in reduced_df.collect():
            self._sent_to_client(row)

    def _sent_to_client(self, data):
        print("THIS IS LEGIT CLIENT")
        print(data["payload"])
