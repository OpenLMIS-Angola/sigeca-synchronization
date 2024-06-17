import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import uuid
from pyspark.sql.functions import col, udf
from app.domain.resources import (
    FacilityResourceRepository,
    GeographicZoneResourceRepository,
    FacilityOperatorResourceRepository,
    ProgramResourceRepository,
    FacilityTypeResourceRepository,
)
from app.infrastructure.sigeca_api_client import SigecaApiClient
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
    ArrayType,
    DoubleType,
    DataType
)
from app.infrastructure.jdbc_reader import JDBCReader
from .validators import validate_facilities_dataframe
from .geo_zones import GeoZoneSynchronization
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
           ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("code", StringType(), True)
    ])),
            True,
        ),
    ]
)


class FacilitySynchronizationService:
    def __init__(
        self,
        jdbc_reader: JDBCReader,
        facility_client: SigecaApiClient,
        facility_repository: FacilityResourceRepository,
        geo_zone_repo: GeographicZoneResourceRepository,
        facility_type_repo: FacilityTypeResourceRepository,
        operator_repo: FacilityOperatorResourceRepository,
        program_repo: ProgramResourceRepository,
    ):
        self.facility_client = facility_client
        self.facility_repository = facility_repository
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
                "left"
            )

            create, update, delete = self._split_df(joined)

            self._create_new_facilities(create)
            self._update_existing_facilities(update)
            self._delete_removed_facilities(delete)

            raise ValueError("Fail")

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
        df = validate_facilities_dataframe(df)
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
        code_id_dict = {row['code']: row['id'] for row in programs.collect()}

        add_info = udf(lambda supported_programs: {entry['code']: code_id_dict.get(entry['code'], None) for entry in supported_programs if entry['code'] in code_id_dict})
        df = df.withColumn("code_id_dict", add_info(col("services")))
        return df 


    def fetch_existing_data(self, query):
        query = repository.generate_sql_query()
        return self.spark.read.jdbc(
            url=self.db_url, table=f"({query}) AS tmp", properties=self.jdbc_properties
        ).collect()

    def synchronize_mising_geographic_zones(self, df: DataFrame):
        GeoZoneSynchronization(
            self.jdbc_reader, df, self.geo_zone_repo
        ).synchronize_missing_geo_zones()

    def synchronize_mising_types(self, df):
        FacilityTypeSynchronization(
            self.jdbc_reader, df, self.facility_type_repo
        ).synchronize()

    def synchronize_products(self, df):
        ProgramSynchronization(self.jdbc_reader, df, self.program_repo).synchronize()

    def _split_df(self, df):
        deleted = df.filter(col("facilities.is_deleted") == True)
        existing = df.filter(col("facilities.is_deleted") == False)

        new_facilities = existing.filter(col("existing_facilities.id").isNull())
        updated_facilities = existing.filter(col("existing_facilities.id").isNotNull())
        return new_facilities, updated_facilities, deleted

    def _create_new_facilities(self, facilities):
        format_payload_f = udf(
            lambda name, code, geographic_zone, facility_type, supported_programs: json.dumps(
                {
                    "id": str(uuid.uuid4()),
                    "code": code,
                    "name": name,
                    "geographicZone": {"id": geographic_zone},
                    "type": {"id": facility_type},
                    "active": True,
                    "enabled": True,
                    "supportedPrograms": [
                        {"id": program_id} for program_id in json.loads(supported_programs).values()
                    ],
                }
            )
        )
        df = (
            facilities.withColumn(
                "payload",
                format_payload_f(
                    col(f"facilities.name"),
                    col(f"facilities.code"),
                    col("municipality.id"),
                    col("facility_type.id"),
                    col("code_id_dict"),
                ),
            )
        )


        for row in df.collect():
            self._sent_to_client(row)

    def _sent_to_client(self, data):
        print("THIS IS LEGIT CLIENT")
        print(data["payload"])


    def _update_existing_facilities(self, facilities):
        pass

    def _delete_removed_facilities(self, facilities):
        pass
