from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, udf
import logging
import re
from pyspark.sql.types import BooleanType


def validate_facilities_dataframe(df: DataFrame) -> DataFrame:
    # Regular expression to match valid latitude and longitude
    logger = logging.getLogger(__name__)

    # Check for completely invalid entries: 'name' and 'code' are missing, and both 'municipality' and 'province' are missing
    invalid_entries = df.filter(
        (col("name").isNull() | (col("name") == ""))
        | (col("code").isNull() | (col("code") == ""))
        | (col("municipality").isNull() | (col("municipality") == ""))
        | (col("province").isNull() | (col("province") == ""))
    )

    # Log the number of completely invalid entries
    num_invalid_entries = invalid_entries.count()
    if num_invalid_entries > 0:
        logger.warning(f"Found {num_invalid_entries} invalid entries:")
        # Log details of invalid entries
        invalid_entries[["code", "name", "municipality", "province"]].show(
            truncate=False
        )
    else:
        logger.info("All entries have mandatory fields.")

    # Check if 'name' and 'code' are not empty or null
    df = df.filter(
        col("name").isNotNull()
        & (col("name") != "")
        & col("code").isNotNull()
        & (col("code") != "")
    )

    # Check if either 'municipality' or 'province' is provided
    df = df.filter(
        (col("municipality").isNotNull() & (col("municipality") != ""))
        & (col("province").isNotNull() & (col("province") != ""))
    )

    # Check if 'is_deleted' equals false
    df = df.filter(col("is_deleted") == False)

    # Validate and convert 'latitude' and 'longitude'
    df = df.withColumn(
        "latitude_valid",
        when(col("latitude").rlike(r"^-?\d+(\.\d+)?$"), True).otherwise(False),
    )
    df = df.withColumn(
        "longitude_valid",
        when(col("longitude").rlike(r"^-?\d+(\.\d+)?$"), True).otherwise(False),
    )

    # Log invalid latitude and longitude entries
    invalid_lat_long = df.filter(~col("latitude_valid") | ~col("longitude_valid"))
    num_invalid_lat_long = invalid_lat_long.count()
    if num_invalid_lat_long > 0:
        logger.warning(
            f"Found {num_invalid_lat_long} entries with invalid latitude or longitude:"
        )
        # Log details of invalid entries
        invalid_lat_long[["code", "latitude", "longitude"]].show(truncate=False)
    else:
        logger.info("All entries have latitude and longitude in valid format.")

    # Convert valid latitude and longitude to double, set invalid to null
    df = df.withColumn(
        "facilities.latitude",
        when(col("latitude_valid"), col("latitude").cast("double")).otherwise(None),
    )
    df = df.withColumn(
        "facilities.longitude",
        when(col("longitude_valid"), col("longitude").cast("double")).otherwise(None),
    )

    # Drop helper columns
    df = df.drop("latitude_valid", "longitude_valid")

    return df
