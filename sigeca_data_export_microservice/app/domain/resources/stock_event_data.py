from datetime import datetime
from uuid import uuid4
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType

from .abstract import ResourceReader
from .util import schema_map, table_map, map_data
from ...infrastructure import ChangeLogOperation


class StockEventResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "stock_events"

    @classmethod
    def read_schema_name(cls):
        return "stockmanagement"

    def read_schema(self):
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("documentnumber", StringType(), True),
                StructField("facilityid", StringType(), True),
                StructField("processeddate", TimestampType(), True),
                StructField("programid", StringType(), True),
                StructField("signature", StringType(), True),
                StructField("userid", StringType(), True),
                StructField("isshowed", BooleanType(), True),
                StructField("isactive", BooleanType(), True)
            ]
        )

    def transform_data(self, df):
        df = df.withColumn("processeddate", df["processeddate"].cast('string'))

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
