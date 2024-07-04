import json
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf, from_json, when
from pyspark.sql.types import StringType

from app.application.synchronization.facilities import (
    FacilitySupplementSync,
    FacilityDataTransformer,
)
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
from .data_transformator import FacilityDataTransformer, get_format_payload_f


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
        self.supplement_sync = FacilitySupplementSync(
            self.jdbc_reader,
            self.lmis_client,
            self.geo_zone_repo,
            self.facility_type_repo,
            self.program_repo,
        )

        self.facility_data_transformer = FacilityDataTransformer(
            self.jdbc_reader,
            self.geo_zone_repo,
            self.facility_type_repo,
            self.program_repo,
        )

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

            logging.info("Synchronizing Facilities")
            self._create_new_facilities(create)
            logging.info("Updating Facilities")
            self._update_existing_facilities(update)

            logging.info("Deactivating Deleted Facilities")
            self._delete_removed_facilities(delete)

            # Log the results
            logging.info("Facility synchronization completed successfully")

        except Exception as e:
            logging.error(f"An error occurred during facility synchronization: {e}")
            raise

    def validate_and_transform(self, facilities):
        # Extract services names from the nested structure
        config = True
        if config is True:
            self.synchronize_supplement_data(facilities)

        df = self.get_full_facilities_data_frame(facilities)

        return df

    def get_full_facilities_data_frame(self, facilities):
        return self.facility_data_transformer.get_data_frame_with_full_information(
            facilities
        )

    def synchronize_supplement_data(self, facilities):
        df = self.facility_data_transformer.get_validated_dataframe(facilities)
        # Check for mandatory fields and valid relations
        self.supplement_sync.synchronize_supplement_data(df)
        return df

    def _split_df(self, df):
        deleted = df.filter(col("facilities.is_deleted") == True).filter(
            col("existing_facilities.id").isNotNull()
        )

        existing = df.filter(col("facilities.is_deleted") == False)
        new_facilities = existing.filter(col("existing_facilities.id").isNull())
        updated_facilities = existing.filter(col("existing_facilities.id").isNotNull())
        return new_facilities, updated_facilities, deleted

    def _create_new_facilities(self, facilities):
        format_payload_f = get_format_payload_f()
        df = facilities.withColumn(
            "payload",
            format_payload_f(
                col(f"existing_facilities.id"),
                col(f"facilities.name"),
                col(f"facilities.code"),
                col("municipality.id"),
                col("facility_type.id"),
                lit("{}"),
                col("is_operational"),
                lit(True),
            ),
        )
        logging.info(f"New Facilities to be created: { df.count()}")
        for row in df.collect():
            self._create_request(row)

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

        format_payload_f = get_format_payload_f()

        facilities = facilities.withColumn(
            "payload",
            format_payload_f(
                col(f"existing_facilities.id"),
                col(f"facilities.name"),
                col(f"facilities.code"),
                col("municipality.id"),
                col("facility_type.id"),
                col("existing_facilities.supported_programs"),  # Use Existing Services
                col("is_operational"),
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
                col("existing_facilities.active"),
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
                # Skip supported programs as they remain the same.
                if field_name == "supportedPrograms":
                    continue
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

        logging.info(f"Comparing Changes For Facilities: {facilities.count()}")
        # Apply the comparison function
        result_df = compare_for_any_change(facilities, "json1_struct", "json2_struct")

        result_df = result_df.filter(col("any_change") == True)[
            ["payload", "existing_facilities.id"]
        ]

        logging.info(f"Facilities That Changed since last update: {result_df.count()}")
        for row in result_df.collect():
            self._update_request(row)

    def _delete_removed_facilities(self, facilities):
        # Delete doesn't remove the facility, it set's it to disaled and unactive
        self._update_existing_facilities(facilities, True)
