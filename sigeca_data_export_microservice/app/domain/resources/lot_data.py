from datetime import datetime
from uuid import uuid4

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

from .abstract import ResourceReader
from .util import schema_map, table_map, map_data
from ...infrastructure import ChangeLogOperation


class LotResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "lots"

    @classmethod
    def read_schema_name(cls):
        return "referencedata"

    def read_schema(self):
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("lotcode", StringType(), True),
                StructField("expirationdate", DateType(), True),
                StructField("manufacturedate", DateType(), True),
                StructField("tradeitemid", StringType(), True),
                StructField("active", BooleanType(), True),
                StructField("quarantined", BooleanType(), True)
            ]
        )

    def transform_data(self, df):
        df = df.withColumn("expirationdate", col("expirationdate").cast("string")) \
            .withColumn("manufacturedate", col("manufacturedate").cast("string"))

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
