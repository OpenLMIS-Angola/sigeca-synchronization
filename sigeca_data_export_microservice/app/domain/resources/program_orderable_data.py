from datetime import datetime
from uuid import uuid4

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DecimalType, LongType

from app.domain.resources.abstract import ResourceReader
from app.domain.resources.util import table_map, schema_map, map_data
from app.infrastructure import ChangeLogOperation


class ProgramOrderableResourceReader(ResourceReader):
    pass

    @classmethod
    def read_table_name(cls):
        return "program_orderables"

    @classmethod
    def read_schema_name(cls):
        return "referencedata"

    def read_schema(self):
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("active", BooleanType(), True),
                StructField("displayorder", IntegerType(), True),
                StructField("dosesperpatient", IntegerType(), True),
                StructField("fullsupply", BooleanType(), True),
                StructField("priceperpack", DecimalType(19, 2), True),
                StructField("orderabledisplaycategoryid", StringType(), True),
                StructField("orderableid", StringType(), True),
                StructField("programid", StringType(), True),
                StructField("orderableversionnumber", LongType(), True)
            ]
        )

    def transform_data(self, df):
        df = df.withColumn("priceperpack", col("priceperpack").cast("string"))

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
