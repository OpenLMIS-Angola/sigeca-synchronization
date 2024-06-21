import json
from uuid import uuid4
from pyspark.sql.types import TimestampType, IntegerType, StructType, StructField, StringType

from .abstract import ResourceReader
from app.domain.resources.util import schema_map, table_map, map_data


class ChangeLogResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "data_changes"

    @classmethod
    def read_schema_name(cls):
        return "changelog"

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
        return df.rdd.map(self._to_payload()).collect()

    @staticmethod
    def _to_payload():
        def map_schema(row):
            return {
                "id": str(uuid4()),
                "schema_name": schema_map[(row.schema_name, row.table_name)],
                "table_name": table_map[(row.schema_name, row.table_name)],
                "operation": row.operation,
                "change_time": row.change_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "row_data": map_data(json.loads(row.row_data), row.schema_name, row.table_name)
            }

        return map_schema
