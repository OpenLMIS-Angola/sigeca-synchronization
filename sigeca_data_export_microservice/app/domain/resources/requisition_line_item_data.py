from datetime import datetime
from uuid import uuid4

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, LongType, DecimalType

from .abstract import ResourceReader
from .util import schema_map, table_map, map_data
from ...infrastructure import ChangeLogOperation


class RequisitionLineItemResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "requisition_line_items"

    @classmethod
    def read_schema_name(cls):
        return "requisition"

    def read_schema(self):
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("adjustedconsumption", IntegerType(), True),
                StructField("approvedquantity", IntegerType(), True),
                StructField("averageconsumption", IntegerType(), True),
                StructField("beginningbalance", IntegerType(), True),
                StructField("calculatedorderquantity", IntegerType(), True),
                StructField("maxperiodsofstock", DecimalType(38, 18), True),
                StructField("maximumstockquantity", IntegerType(), True),
                StructField("nonfullsupply", BooleanType(), True),
                StructField("numberofnewpatientsadded", IntegerType(), True),
                StructField("orderableid", StringType(), True),
                StructField("packstoship", LongType(), True),
                StructField("priceperpack", DecimalType(19, 2), True),
                StructField("remarks", StringType(), True),
                StructField("requestedquantity", IntegerType(), True),
                StructField("requestedquantityexplanation", StringType(), True),
                StructField("skipped", BooleanType(), True),
                StructField("stockonhand", IntegerType(), True),
                StructField("total", IntegerType(), True),
                StructField("totalconsumedquantity", IntegerType(), True),
                StructField("totalcost", DecimalType(19, 2), True),
                StructField("totallossesandadjustments", IntegerType(), True),
                StructField("totalreceivedquantity", IntegerType(), True),
                StructField("totalstockoutdays", IntegerType(), True),
                StructField("requisitionid", StringType(), True),
                StructField("idealstockamount", IntegerType(), True),
                StructField("calculatedorderquantityisa", IntegerType(), True),
                StructField("additionalquantityrequired", IntegerType(), True),
                StructField("orderableversionnumber", LongType(), True),
                StructField("facilitytypeapprovedproductid", StringType(), True),
                StructField("facilitytypeapprovedproductversionnumber", LongType(), True),
                StructField("numberofpatientsontreatmentnextmonth", IntegerType(), True),
                StructField("totalrequirement", IntegerType(), True),
                StructField("totalquantityneededbyhf", IntegerType(), True),
                StructField("quantitytoissue", IntegerType(), True),
                StructField("convertedquantitytoissue", IntegerType(), True)
            ]
        )

    def transform_data(self, df):
        df = df.withColumn("maxperiodsofstock", col("maxperiodsofstock").cast('string')) \
            .withColumn("totalcost", col("totalcost").cast('string')) \
            .withColumn("priceperpack", col("priceperpack").cast('string'))

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
