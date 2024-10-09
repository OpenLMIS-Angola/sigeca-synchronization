import json

from pyspark.sql.functions import col, udf

from app.application.synchronization.facilities.facility_schema import facility_schema
from app.config import Config
from app.domain.resources import (
    GeographicZoneResourceRepository,
    FacilityTypeResourceRepository,
    ProgramResourceRepository,
)
from app.infrastructure.jdbc_reader import JDBCReader
from app.application.synchronization.validators import validate_facilities_dataframe


class FacilityDataTransformer:
    def __init__(
        self,
        jdbc_reader: JDBCReader,
        geo_zone_repo: GeographicZoneResourceRepository,
        facility_type_repo: FacilityTypeResourceRepository,
        program_repo: ProgramResourceRepository,
    ):
        self.jdbc_reader = jdbc_reader
        self.geo_zone_repo = geo_zone_repo
        self.facility_type_repo = facility_type_repo
        self.program_repo = program_repo

    def get_data_frame_with_full_information(self, facilities):
        df = self.get_validated_dataframe(facilities)
        df = self._add_zones_to_df(df)
        df = self._add_types_to_df(df)
        df = self._add_supported_program_info_to_df(df)
        return df

    def get_validated_dataframe(self, facilities):
        df = self.jdbc_reader.spark.createDataFrame(
            facilities, schema=facility_schema
        ).alias("facilities")
        df = validate_facilities_dataframe(df)
        return df

    def _add_zones_to_df(self, df):
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

    def _add_types_to_df(self, df):
        types = self.facility_type_repo.get_all().alias("facility_type")
        df = df.join(types, df["category"] == types["name"], "left")
        return df

    def _add_supported_program_info_to_df(self, df):
        programs = self.program_repo.get_all().alias("program")
        code_id_dict = {row["code"]: row["id"] for row in programs.collect()}

        add_info = udf(
            lambda supported_programs: json.dumps(
                {
                    entry["code"]: {"id": code_id_dict.get(entry["code"], None)}
                    for entry in supported_programs
                    if entry["code"] in code_id_dict
                }
            )
        )
        df = df.withColumn("code_id_dict", add_info(col("services")))
        return df


def get_format_payload_f():
    fallbacks = Config().fallbacks
    format_payload_f = udf(
        lambda id, name, code, geographic_zone, facility_type, supported_programs, operational, enabled: json.dumps(
            {
                "id": id,
                "code": code,
                "name": name,
                "geographicZone": {"id": geographic_zone or fallbacks.geographicZone},
                "type": {"id": facility_type or fallbacks.type},
                "active": operational,
                "enabled": enabled,
                "openLmisAccessible": enabled,
                "supportedPrograms": [
                    {
                        "id": data["id"],
                        "supportActive": data.get("supportActive", True),
                        "supportLocallyFulfilled": data.get(
                            "supportLocallyFulfilled", False
                        ),
                        "supportStartDate": data.get("supportStartDate"),
                    }
                    for data in json.loads(supported_programs).values()
                    if "id" in data.keys()
                ],
            }
        )
    )

    return format_payload_f


def get_email_response_f():
    return udf(
        lambda name, code, municipality_name, facility_type, operation: json.dumps(
            {
                "name": name,
                "code": code,
                "municipality": municipality_name or "UNDEFINED",
                "type": facility_type or "UNDEFINED",
                "operation": operation,
            }
        )
    )
