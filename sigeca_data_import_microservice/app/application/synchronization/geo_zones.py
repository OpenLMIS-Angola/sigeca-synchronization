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
from .abstract import FacilitySupplementSynchronization

logger = logging.getLogger(__name__)


class GeoZoneSynchronization(FacilitySupplementSynchronization):
    endpoint = "geographicZones"

    @property
    def geo_location_level(self):
        raise NotImplementedError()

    @property
    def sigeca_location_alias(self):
        raise NotImplementedError()

    def add_parent(self, df):
        raise NotImplementedError()

    def get_level_id(self):
        levels = self.repo.get_levels()
        return levels.filter(col("levelnumber") == self.geo_location_level).first().id

    def add_code(self, reduced_df):
        indicator = self.sigeca_location_alias[0]
        create_codes_f = udf(
            lambda x: f"gz-{unidecode.unidecode(x.lower())}-{indicator}"
        )
        reduced_df = reduced_df.withColumn(
            "code", create_codes_f(col(f"facilities.{self.sigeca_location_alias}"))
        )

        return reduced_df

    def _add_missing(self):
        try:
            df = self._create_joined_df()
            missing = self.validate(df)
            # Expected only one highest level region
            missing = self.add_parent(missing)
            self._add_missing_geo_location(missing)
        except Exception as e:
            logging.error(f"An error occurred during facility synchronization: {e}")
            raise

    def _create_joined_df(self):
        df = self.facilities.alias("facilities")
        geo_zone_df = self.repo.get_all().alias("geo_zone")
        relevant_zones = geo_zone_df.filter(
            col("levelnumber") == self.geo_location_level
        ).alias("geo_zone")

        # Validate foreign keys
        df = df.join(
            relevant_zones,
            df[f"facilities.{self.sigeca_location_alias}"] == relevant_zones["name"],
            "left",
        )
        return df

    def validate(self, facilities_df):
        # Extract services names from the nested structure
        missing = facilities_df.filter((col(f"geo_zone.id").isNull()))
        num_invalid_lat_long = missing.count()
        if num_invalid_lat_long > 0:
            logger.warning(
                f"Found {num_invalid_lat_long} facilities with non existing province present:"
            )
            # Log details of invalid entries
            missing.distinct()[
                ["facilities.code", "facilities.name", f"facilities.province"]
            ].show()
        else:
            logger.info(f"All {self.sigeca_location_alias} matching.")
        return missing

    def _add_missing_geo_location(self, missing: DataFrame):
        level_id = self.get_level_id()
        reduced_df = missing[[f"facilities.{self.sigeca_location_alias}", "parent_id"]]
        add_level_id_f = udf(lambda: level_id)
        format_payload = udf(
            lambda name, code, level, parent: json.dumps(
                {
                    "code": code,
                    "name": name,
                    "level": {"id": level},
                    "parent": {"id": parent},
                }
            )
        )

        reduced_df = self.add_code(reduced_df)
        reduced_df = reduced_df.withColumn("levelid", add_level_id_f())
        reduced_df = reduced_df.withColumn(
            "payload",
            format_payload(
                col(f"facilities.{self.sigeca_location_alias}"),
                col("code"),
                col("levelid"),
                col("parent_id"),
            ),
        )

        reduced_df = reduced_df[["payload"]].distinct()
        for row in reduced_df.collect():
            try:
                self._sent_to_client(row)
            except Exception as e:
                logger.warning(f"Failed to synch resource: {row}\nReason: {e}")

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


class ProvinceSynchronization(GeoZoneSynchronization):
    endpoint = "geographicZones"
    geo_location_level = 2
    sigeca_location_alias = "province"

    def add_parent(self, df):
        parent = (
            self.repo.get_all().filter(col("levelnumber") == 1).alias("region").first()
        )
        add_parent_id = udf(lambda: parent["id"])
        missing = df.withColumn("parent_id", add_parent_id())
        return missing

    def get_level_id(self):
        levels = self.repo.get_levels()
        return levels.filter(col("levelnumber") == self.geo_location_level).first().id


class MunicipalitySynchronization(GeoZoneSynchronization):
    endpoint = "geographicZones"
    geo_location_level = 3
    sigeca_location_alias = "municipality"

    def add_parent(self, df):
        parents = (
            self.repo.get_all().filter(col("levelnumber") == 2).alias("parents")
        )[["name", "id"]].collect()
        parents_dict = {}
        for row in parents:
            parents_dict[row["name"]] = row["id"]

        add_parent_id = udf(lambda province: parents_dict[province])
        missing = df.withColumn("parent_id", add_parent_id(col("province")))
        return missing

    def get_level_id(self):
        levels = self.repo.get_levels()
        return levels.filter(col("levelnumber") == self.geo_location_level).first().id
