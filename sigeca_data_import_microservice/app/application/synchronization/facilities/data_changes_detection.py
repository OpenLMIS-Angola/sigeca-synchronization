import json
import logging
from collections import defaultdict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf, from_json, when

from app.config import Config

x = defaultdict()


def get_fallbacks():
    return Config().fallbacks


def basic_comparison(column):
    return lambda col1, col2: col(f"{col1}.{column}") != col(f"{col2}.{column}")


def basic_with_fallback(column):
    # For the resources that doesn't have internal object and fallback to default one (like UNKNOWN)
    # but was reassigned in the system to other, actual object.
    fallback_value = get_fallbacks() or {}

    return lambda col1, col2: (
        (col(f"{col1}.{column}") != col(f"{col2}.{column}"))
        & (col(f"{col1}.{column}.id") != lit(fallback_value[column]))
    )


def get_default_field_comparison_strategy():
    return {
        "supportedPrograms": lambda col1, col2: lit(
            False
        ),  # Skip comparison of the supported programs, always true
        "name": basic_comparison("name"),
        "code": basic_comparison("code"),
        "active": basic_comparison("active"),
        "enabled": basic_comparison("enabled"),
        "openLmisAccessible": basic_comparison("openLmisAccessible"),
        "geographicZone": basic_with_fallback("geographicZone"),
        "type": basic_with_fallback("type"),
    }


def compare_json_content(
    facilities: DataFrame,
    json_1_column_name: str,
    json_2_column_name: str,
    comparison_strategy: dict = None,
):
    if not comparison_strategy:
        comparison_strategy = get_default_field_comparison_strategy()

    def compare_for_any_change(df, col1, col2):
        changes = []
        for field, strategy in comparison_strategy.items():
            # Skip supported programs as they remain the same.
            changes.append(strategy(col1, col2))

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
        return df.withColumn("any_change", change_column)

    logging.info(f"Comparing Changes For Facilities: {facilities.count()}")
    # Apply the comparison function
    result_df = compare_for_any_change(
        facilities, json_1_column_name, json_2_column_name
    )
    return result_df
