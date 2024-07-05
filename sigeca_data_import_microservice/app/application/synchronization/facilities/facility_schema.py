from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    ArrayType,
)

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
