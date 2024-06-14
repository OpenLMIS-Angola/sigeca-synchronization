from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DateType,
    StringType as UUIDType,
)
from pyspark.sql.functions import col, lit, current_date, expr
import requests
import json
import pprint
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Callable, Any

from .abstract import ResourceReader


class FacilityResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "facilities"

    @classmethod
    def read_schema_name(cls) -> StructType:
        return "referencedata"

    def read_schema(self):
        return StructType(
            [
                StructField("id", UUIDType(), True),
                StructField("active", BooleanType(), True),
                StructField("code", StringType(), True),
                StructField("comment", StringType(), True),
                StructField("description", StringType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("godowndate", DateType(), True),
                StructField("golivedate", DateType(), True),
                StructField("name", StringType(), True),
                StructField("openlmisaccessible", BooleanType(), True),
                StructField("geographiczoneid", UUIDType(), True),
                StructField("operatedbyid", UUIDType(), True),
                StructField("typeid", UUIDType(), True),
                StructField("extradata", StringType(), True),
                StructField(
                    "location", StringType(), True
                ),  # Placeholder for user-defined type
            ]
        )

    def transform_data(self, df):
        # Generate a new UUID for the id column
        generate_uuid = expr("uuid()")

        # Apply transformations to match the target schema
        transformed_df = (
            df.withColumn("id", generate_uuid)
            .withColumn("reference_id", col("id"))
            .withColumn("last_updated", current_date())
            .withColumn("is_deleted", lit(False))
            .select(
                col("id"),
                col("reference_id"),
                col("active"),
                col("code"),
                col("comment"),
                col("geographiczoneid").alias("geographic_zone_id"),
                col("description"),
                col("enabled"),
                col("name"),
                col("last_updated"),
                col("is_deleted"),
            )
        )

        return transformed_df
