from pyspark.sql import SparkSession
from pyspark.sql.types import (
    TimestampType,
    IntegerType,
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


class CachangeLogResourceReader(ResourceReader):
    def read_table_name(self):
        return "changelog"

    def read_schema_name(self) -> StructType:
        return "data_changes"

    def read_schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("schema_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("operation", StringType(), True),
                StructField("change_time", TimestampType(), True),
                StructField("row_data", StringType(), True),
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
