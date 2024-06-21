from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from datetime import datetime
from .utils import ChangeLogOperation
import re


class JDBCReader:
    """
    JDBCReader is a utility class designed to facilitate reading data from a JDBC source into Spark DataFrames.
    It supports fetching entire tables and extracting changes based on a timestamp, while also handling JSONB
    column parsing and transformation.
    """

    def __init__(self, config: Any):
        """
        Initializes the JDBCReader with the provided configuration and sets up the Spark session.

        Args:
            config (Any): A dictionary containing JDBC connection parameters, such as:
                          - 'jdbc_url': The JDBC URL for the database connection.
                          - 'jdbc_user': The username for the database connection.
                          - 'jdbc_password': The password for the database connection.
                          - 'jdbc_driver': The JDBC driver class name.
        """
        self.config = config
        self.spark = (
            SparkSession.builder.appName("DataSyncMicroservice")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel(config.get('log_level', 'WARN'))

    def read_data(self, table_name: str, schema: StructType) -> DataFrame:
        """
        Reads data from a specified table in the database and returns it as a Spark DataFrame.

        Args:
            table_name (str): The name of the table to read data from.
                              If the table is in a schema other than public, it has to be specified as schema.table.
            schema (StructType): The schema of the table to enforce during read.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the specified table.
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config["jdbc_url"])
            .option("dbtable", table_name)
            .option("user", self.config["jdbc_user"])
            .option("password", self.config["jdbc_password"])
            .option("driver", self.config["jdbc_driver"])
            .schema(schema)
            .load()
        )

    def read_changes(
        self,
        table_name: str,
        schema: StructType,
        operation: ChangeLogOperation = None,
        last_sync_timestamp: Optional[datetime] = None,
    ) -> DataFrame:
        """
        Reads changes from a specified table based on the last synchronization timestamp and parses the JSONB column.

        Args:
            table_name (str): The name of the table to read changes from. Should be in the format 'schema.table'.
            schema (StructType): The schema defining the structure of the JSONB column.
            operation (ChangeLogOperationEnum, optional): The type of operation to filter by (INSERT, UPDATE, DELETE).
            last_sync_timestamp (datetime, optional): The timestamp indicating the last synchronization time.
                                                      Fetches all changes if not provided.

        Returns:
            DataFrame: A Spark DataFrame containing the changes since the last synchronization,
                       with the JSONB column parsed into individual fields.
        """
        schema_name, table_name = self._sanitize_sql_identifier(table_name).split(".")
        query = f"(SELECT * FROM changelog.data_changes WHERE schema_name = '{schema_name}' AND table_name = '{table_name}'"

        if operation:
            query += f" AND operation = '{operation.value}'"
        if last_sync_timestamp:
            last_sync_timestamp = last_sync_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            query += f" AND change_time > '{last_sync_timestamp}'"

        query += ") AS tmp"

        base_df = (
            self.spark.read.format("jdbc")
            .option("url", self.config["jdbc_url"])
            .option("dbtable", query)
            .option("user", self.config["jdbc_user"])
            .option("password", self.config["jdbc_password"])
            .option("driver", self.config["jdbc_driver"])
            .load()
            .withColumn("parsed_json", from_json(col("row_data"), schema))
        )

        return self._selection_transformation(base_df, schema)

    def _selection_transformation(self, df: DataFrame, schema: StructType) -> DataFrame:
        """
        Transforms the DataFrame by selecting and aliasing columns from the parsed JSONB data
        based on the provided schema.

        Args:
            df (DataFrame): The DataFrame containing the parsed JSONB data.
            schema (StructType): The schema defining the fields to select from the parsed JSONB data.

        Returns:
            DataFrame: A Spark DataFrame with columns matching the schema field names, extracted from the JSONB data.
        """
        select_expr = [
            col(f"parsed_json.{field.name}").alias(field.name)
            for field in schema.fields
        ]
        return df.select(*select_expr)

    def _sanitize_sql_identifier(self, identifier: str) -> str:
        """
        Sanitizes SQL identifiers to prevent SQL injection.

        Args:
            identifier (str): The SQL identifier to sanitize.

        Returns:
            str: The sanitized SQL identifier.
        """
        # Allow only alphanumeric characters and underscores, disallow other characters
        if not re.match(r"^[A-Za-z0-9_\.]+$", identifier):
            raise ValueError(f"Invalid SQL identifier: {identifier}")
        return identifier
