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
)
from app.infrastructure.sigeca_api_client import SigecaApiClient

from app.infrastructure.jdbc_reader import JDBCReader
from .validators import validate_facilities_dataframe
import unidecode
import json

logger = logging.getLogger(__name__)


class GeoZoneSynchronization:
    def __init__(
        self,
        jdbc_reader: JDBCReader,
        facilities: DataFrame,
        geo_zone_repo: GeographicZoneResourceRepository,
    ):
        self.facilities = facilities
        self.geo_zone_repo = geo_zone_repo
        self.jdbc_reader = jdbc_reader

    def synchronize_missing_geo_zones(self):
        try:
            self._add_missing_provinces()
            self._add_missing_municipalities()
            logging.info("Facility synchronization completed successfully")
        except Exception as e:
            logging.error(f"An error occurred during facility synchronization: {e}")
            raise

    def _create_joined_df(self):
        df = self.facilities.alias("facilities")
        geo_zone_df = self.geo_zone_repo.get_all().alias("geo_zone")
        municipalities = geo_zone_df.filter(col("levelnumber") == 3).alias(
            "municipality"
        )
        province = geo_zone_df.filter(col("levelnumber") == 2).alias("province")

        # Validate foreign keys
        df = df.join(
            municipalities, df["municipality"] == municipalities["name"], "left"
        )
        df = df.join(province, df["province"] == province["name"], "left")
        return df

    def validate(self, facilities_df):
        # Extract services names from the nested structure
        missing_municipality = self._validate_geo_zone_on_level(
            facilities_df, "municipality"
        )
        missing_province = self._validate_geo_zone_on_level(facilities_df, "province")
        return missing_municipality, missing_province

    def _add_missing_municipalities(self):
        df = self._create_joined_df()
        levels = self.geo_zone_repo.get_levels()
        level_id = levels.filter(col("levelnumber") == 3).first().id
        missing_municipality, missing_province = self.validate(df)

        geo_zone_df = (
            self.geo_zone_repo.get_all()
            .alias("geo_zone")
            .filter(col("levelnumber") == 2)[["id", "name", "levelnumber"]]
            .collect()
        )

        parents = {}
        for item in geo_zone_df:
            parents[item.name] = item.id

        add_parent_id = udf(lambda province: parents.get(province, None))
        missing_municipality = missing_municipality.withColumn(
            "parent_id", add_parent_id(col("province"))
        )
        self._add_missing_geo_location(missing_municipality, level_id, "municipality")

    def _add_missing_provinces(
        self,
    ):
        df = self._create_joined_df()
        levels = self.geo_zone_repo.get_levels()
        level_id = levels.filter(col("levelnumber") == 2).first().id
        missing_municipality, missing_province = self.validate(df)
        # Expected only one highest level region
        geo_zone_df = self.geo_zone_repo.get_all().alias("geo_zone")
        region = geo_zone_df.filter(col("levelnumber") == 1).alias("region").first()
        add_parent_id = udf(lambda: region["id"])
        missing_province = missing_province.withColumn("parent_id", add_parent_id())

        self._add_missing_geo_location(missing_province, level_id, "province")

    def _add_missing_geo_location(
        self, missing: DataFrame, level_id: str, level_name: str
    ):
        reduced_df = missing[[f"facilities.{level_name}", "parent_id"]]
        create_codes_f = udf(lambda x: f"gz-{unidecode.unidecode(x.lower())}")
        add_level_id_f = udf(lambda: level_id)
        format_payload = udf(
            lambda name, code, level, parent: json.dumps(
                {"code": code, "name": name, "level": {"id": level}, "parent": parent}
            )
        )
        reduced_df = reduced_df.withColumn(
            "code", create_codes_f(col(f"facilities.{level_name}"))
        )
        reduced_df = reduced_df.withColumn("levelid", add_level_id_f())
        reduced_df = reduced_df.withColumn(
            "payload",
            format_payload(
                col(f"facilities.{level_name}"),
                col("code"),
                col("levelid"),
                col("parent_id"),
            ),
        )

        reduced_df = reduced_df.distinct()
        reduced_df.show()
        for row in reduced_df.collect():
            self._sent_to_client(row)

    def _sent_to_client(self, data):
        print("THIS IS LEGIT CLIENT")
        print(data["payload"])

    def _validate_geo_zone_on_level(self, facilities_df, level):
        missing = facilities_df.filter((col(f"{level}.id").isNull()))
        num_invalid_lat_long = missing.count()
        if num_invalid_lat_long > 0:
            logger.warning(
                f"Found {num_invalid_lat_long} facilities with non existing {level} present:"
            )
            # Log details of invalid entries
            missing.distinct()[
                ["facilities.code", "facilities.name", f"facilities.{level}"]
            ].show()
        else:
            logger.info(f"All {level} matching.")
        return missing
