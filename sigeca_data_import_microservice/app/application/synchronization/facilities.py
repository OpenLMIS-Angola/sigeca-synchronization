import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import uuid
from pyspark.sql.functions import (
    col,
    udf,
    from_json,
    when,
    array_except,
    concat,
    size,
    lit,
)
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    ProgramResourceRepository,
    FacilityTypeResourceRepository,
)
from app.infrastructure.sigeca_api_client import SigecaApiClient
from app.infrastructure.open_lmis_api_client import OpenLmisApiClient
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
    ArrayType,
    DoubleType,
    DataType,
)
from app.infrastructure.jdbc_reader import JDBCReader
from .validators import validate_facilities_dataframe
from .geo_zones import ProvinceSynchronization, MunicipalitySynchronization
from .facility_types import FacilityTypeSynchronization
from .product import ProgramSynchronization
import json

facility_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("reference_id", StringType(), True),
        StructField("is_deleted", BooleanType(), True),
        StructField("last_updated", StringType(), True),  # TODO: Change to timestamp
        StructField("name", StringType(), True),
        StructField("code", StringType(), True),
        StructField("acronym", StringType(), True),
        StructField("category", StringType(), True),
        StructField("ownership", StringType(), True),
        StructField("management", StringType(), True),
        StructField("municipality", StringType(), True),
        StructField("province", StringType(), True),
        StructField("is_operational", BooleanType(), True),
        StructField(
            "latitude", StringType(), True
        ),  # Data of latitude and longitude inconsitent
        StructField("longitude", StringType(), True),
        StructField(
            "services",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("code", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


class FacilitySynchronizationService:
    def __init__(
        self,
        jdbc_reader: JDBCReader,
        facility_client: SigecaApiClient,
        lmis_client: OpenLmisApiClient,
        facility_repository: FacilityResourceRepository,
        geo_zone_repo: GeographicZoneResourceRepository,
        facility_type_repo: FacilityTypeResourceRepository,
        operator_repo: FacilityOperatorResourceRepository,
        program_repo: ProgramResourceRepository,
    ):
        self.facility_client = facility_client
        self.facility_repository = facility_repository
        self.lmis_client = lmis_client
        self.geo_zone_repo = geo_zone_repo
        self.facility_type_repo = facility_type_repo
        self.operator_repo = operator_repo
        self.program_repo = program_repo
        self.jdbc_reader = jdbc_reader

    def synchronize_facilities(self):
        try:
            # Step 1: Fetch data from the external API
            external_facilities = self.facility_client.fetch_facilities()

            # Step 2: Validate and transform the data
            valid_external_df = self.validate_and_transform(external_facilities)

            # Step 3: Fetch existing data from the database
            existing_facilities = self.facility_repository.get_all().alias(
                "existing_facilities"
            )

            joined = valid_external_df.join(
                existing_facilities,
                valid_external_df["facilities.code"] == existing_facilities["code"],
                "left",
            )

            create, update, delete = self._split_df(joined)

            self._create_new_facilities(create)
            self._update_existing_facilities(update)
            self._delete_removed_facilities(delete)

            # Log the results
            logging.info("Facility synchronization completed successfully")

        except Exception as e:
            logging.error(f"An error occurred during facility synchronization: {e}")
            raise

    def validate_and_transform(self, facilities):
        # Extract services names from the nested structure
        df = self.jdbc_reader.spark.createDataFrame(
            facilities, schema=facility_schema
        ).alias("facilities")

        # Check for mandatory fields and valid relations
        df = validate_facilities_dataframe(df).filter(
            col("facilities.is_deleted") == False
        )
        self.synchronize_mising_geographic_zones(df)
        self.synchronize_mising_types(df)
        self.synchronize_products(df)

        # Currently skip, on UAT no operators exist
        # self.synchronize_operators()

        df = self.jdbc_reader.spark.createDataFrame(
            facilities, schema=facility_schema
        ).alias("facilities")
        df = validate_facilities_dataframe(df)

        df = self._add_zones_to_df(df)
        df = self._add_types_to_df(df)
        df = self._add_supported_program_info_to_df(df)

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
                    entry["code"]: code_id_dict.get(entry["code"], None)
                    for entry in supported_programs
                    if entry["code"] in code_id_dict
                }
            )
        )
        df = df.withColumn("code_id_dict", add_info(col("services")))
        return df

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

    def _split_df(self, df):
        deleted = df.filter(col("facilities.is_deleted") == True).filter(
            col("existing_facilities.id").isNotNull()
        )
        existing = df.filter(col("facilities.is_deleted") == False)

        new_facilities = existing.filter(col("existing_facilities.id").isNull())
        updated_facilities = existing.filter(col("existing_facilities.id").isNotNull())
        return new_facilities, updated_facilities, deleted

    def _create_new_facilities(self, facilities):
        format_payload_f = self.format_payload_f()
        df = facilities.withColumn(
            "payload",
            format_payload_f(
                col(f"existing_facilities.id"),
                col(f"facilities.name"),
                col(f"facilities.code"),
                col("municipality.id"),
                col("facility_type.id"),
                col("code_id_dict"),
                lit(True),
            ),
        )

        for row in df.collect():
            self._create_request(row)

    def format_payload_f(self):
        format_payload_f = udf(
            lambda id, name, code, geographic_zone, facility_type, supported_programs, enabled: json.dumps(
                {
                    "id": id,
                    "code": code,
                    "name": name,
                    "geographicZone": {"id": geographic_zone},
                    "type": {"id": facility_type},
                    "active": enabled,
                    "enabled": enabled,
                    "supportedPrograms": [
                        {"id": program_id}
                        for program_id in json.loads(supported_programs).values()
                    ],
                }
            )
        )

        return format_payload_f

    def _create_request(self, data):
        try:
            self.lmis_client.send_post_request("facilities", data["payload"])
        except Exception as e:
            logging.error(
                f"An error occurred during facility creation request ({data}): {e}"
            )

    def _update_request(self, data):
        try:
            self.lmis_client.send_put_request("facilities", data["id"], data["payload"])
        except Exception as e:
            logging.error(
                f"An error occurred during facility update request ({data}): {e}"
            )

    def _delete_request(self, data):
        try:
            self.lmis_client.send_delete_request("facilities", data["id"])
        except Exception as e:
            logging.error(
                f"An error occurred during facility delete request ({data}): {e}"
            )

    def merge_json_f(self):
        def _inner_merge(json1, json2):
            dict1 = json.loads(json1)
            dict2 = json.loads(json2)
            merged_dict = {**dict2, **dict1}
            return json.dumps(merged_dict)

        return udf(_inner_merge, StringType())

    def _update_existing_facilities(self, facilities: DataFrame, is_deleted=False):
        merge_json_udf = self.merge_json_f()
        facilities = facilities.withColumn(
            "mergedServices",
            merge_json_udf(
                col("existing_facilities.supported_programs"), col("code_id_dict")
            ),
        )

        format_payload_f = self.format_payload_f()

        facilities = facilities.withColumn(
            "payload",
            format_payload_f(
                col(f"existing_facilities.id"),
                col(f"facilities.name"),
                col(f"facilities.code"),
                col("municipality.id"),
                col("facility_type.id"),
                col("mergedServices"),
                lit(not is_deleted),
            ),
        )

        facilities = facilities.withColumn(
            "oldPayload",
            format_payload_f(
                col(f"existing_facilities.id"),
                col(f"existing_facilities.name"),
                col(f"facilities.code"),
                col("existing_facilities.geographiczoneid"),
                col("existing_facilities.typeid"),
                col("existing_facilities.supported_programs"),
                col("existing_facilities.enabled"),
            ),
        )

        schema = self.jdbc_reader.spark.read.json(
            facilities.rdd.map(lambda row: row.payload)
        ).schema  # Infer schema from the first JSON column
        facilities = facilities.withColumn(
            "json1_struct", from_json(col("payload"), schema)
        )
        facilities = facilities.withColumn(
            "json2_struct", from_json(col("oldPayload"), schema)
        )

        def compare_for_any_change(df, col1, col2):
            changes = []
            for field in schema.fields:
                field_name = field.name
                if field.dataType.simpleString().startswith("array"):
                    changes.append(
                        when(
                            size(
                                concat(
                                    array_except(
                                        f"{col1}.{field_name}", f"{col2}.{field_name}"
                                    ),
                                    array_except(
                                        f"{col2}.{field_name}", f"{col1}.{field_name}"
                                    ),
                                )
                            )
                            == 0,
                            False,
                        ).otherwise(True)
                    )
                else:
                    changes.append(
                        col(f"{col1}.{field_name}") != col(f"{col2}.{field_name}")
                    )

            # Aggregate all change flags into a single boolean indicating any change
            if changes:
                # Aggregate all change flags into a single boolean indicating any change
                change_column = (
                    when(sum([change.cast("int") for change in changes]) > 0, True)
                    .otherwise(False)
                    .alias("any_change")
                )
            else:
                change_column = lit(False).alias(
                    "any_change"
                )  # Handle the case when changes list is empty

            # Select the original JSON payloads and the any_change flag
            return df.select(
                "payload", "existing_facilities.id", "oldPayload", change_column
            )

        # Apply the comparison function
        result_df = compare_for_any_change(facilities, "json1_struct", "json2_struct")
        result_df = result_df.filter(col("any_change") == True)[
            ["payload", "existing_facilities.id"]
        ]
        for row in result_df.collect():
            self._update_request(row)

    def _delete_removed_facilities(self, facilities):
        # Delete doesn't remove the facility, it set's it to disaled and unactive
        self._update_existing_facilities(facilities, True)
