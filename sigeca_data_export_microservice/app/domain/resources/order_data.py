from datetime import datetime
from uuid import uuid4

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, DecimalType

from .abstract import ResourceReader
from .util import schema_map, table_map, map_data
from ...infrastructure import ChangeLogOperation


class OrderResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "orders"

    @classmethod
    def read_schema_name(cls):
        return "fulfillment"

    def read_schema(self):
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("createdbyid", StringType(), True),
                StructField("createddate", TimestampType(), True),
                StructField("emergency", BooleanType(), True),
                StructField("externalid", StringType(), True),
                StructField("facilityid", StringType(), True),
                StructField("ordercode", StringType(), True),
                StructField("processingperiodid", StringType(), True),
                StructField("programid", StringType(), True),
                StructField("quotedcost", DecimalType(19, 2), True),
                StructField("receivingfacilityid", StringType(), True),
                StructField("requestingfacilityid", StringType(), True),
                StructField("status", StringType(), True),
                StructField("supplyingfacilityid", StringType(), True),
                StructField("lastupdateddate", TimestampType(), True),
                StructField("lastupdaterid", StringType(), True),
                StructField("extradata", StringType(), True)
            ]
        )

    def transform_data(self, df):
        df = df.withColumn("createddate", col("createddate").cast("string")) \
            .withColumn("lastupdateddate", col("lastupdateddate").cast("string")) \
            .withColumn("quotedcost", col("quotedcost").cast("string"))

        schema_name = self.read_schema_name()
        table_name = self.read_table_name()

        return df.rdd.map(self._to_payload(table_name, schema_name)).collect()

    def _to_payload(self, table_name, schema_name):
        def map_schema(row):
            return {
                "id": str(uuid4()),
                "schema_name": schema_map[(schema_name, table_name)],
                "table_name": table_map[(schema_name, table_name)],
                "operation": ChangeLogOperation.SYNC.value,
                "change_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "row_data": map_data(row.asDict(), schema_name, table_name),
            }

        return map_schema
