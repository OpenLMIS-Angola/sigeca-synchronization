from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col

class JDBCReader:
    def __init__(self, config: Any):
        self.config = config
        self.spark = SparkSession.builder \
            .appName("DataSyncMicroservice") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
            .getOrCreate()

    def read_data(self, table_name: str, schema: StructType) -> DataFrame:
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config['jdbc_url']) \
            .option("dbtable", table_name) \
            .option("user", self.config['jdbc_user']) \
            .option("password", self.config['jdbc_password']) \
            .option("driver", self.config['jdbc_driver']) \
            .schema(schema) \
            .load()

    def read_changes(self, table_name: str, schema: StructType, last_sync_timestamp: str) -> DataFrame:
        query = f"(SELECT * FROM {table_name} WHERE lastupdateddate > '{last_sync_timestamp}') AS tmp"
        # Base unfolds row_data into parse_json.<schema_field> columns 
        base = self.spark.read \
            .format("jdbc") \
            .option("url", self.config['jdbc_url']) \
            .option("dbtable", query) \
            .option("user", self.config['jdbc_user']) \
            .option("password", self.config['jdbc_password']) \
            .option("driver", self.config['jdbc_driver']) \
            .schema(schema) \
            .load() \
            .withColumn("parsed_json", from_json(col("row_data"), schema))
        
        return self._selection_transformation(base, schema)


    def _selection_transformation(self, df, schema):
        # Removes parse_json prefix in generic manner to match schema 
        select_expr = [col(f"parsed_json.{field.name}").alias(field.name) for field in schema.fields]
        return df.select(*select_expr)