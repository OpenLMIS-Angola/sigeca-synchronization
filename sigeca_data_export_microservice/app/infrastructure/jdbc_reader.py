from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Any
from pyspark.sql import DataFrame

query = f"""
(
    with last_changes as (
        select resource, max("timestamp") as last_change
        from changelog.sync_logs sl
        group by resource
    )
    select dc.* 
    from changelog.data_changes dc
    left join last_changes lc on (dc.schema_name ||'.' || dc.table_name)=lc.resource
    left join last_changes lc2 on lc2.resource = 'changelog.data_changes'
    where dc.change_time > greatest(coalesce(lc.last_change, '1970-01-01'::timestamp), coalesce(lc2.last_change, '1970-01-01'::timestamp))
) AS tmp"""


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

    def read_data(self, table_name: str) -> DataFrame:
        """
        Reads data from a specified table in the database and returns it as a Spark DataFrame.

        Args:
            table_name (str): The name of the table to read data from.
                              If the table is in a schema other than public, it has to be specified as schema.table.
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
            .load()
        )

    def read_changes(self) -> DataFrame:
        """
        Reads changes based on the last synchronization timestamp and parses the JSONB column.

        Args:
            schema (StructType): The schema of the table to enforce during read.
        Returns:
            DataFrame: A Spark DataFrame containing the changes since the last synchronization,
                       with the JSONB column parsed into individual fields.
        """

        return (
            self.spark.read.format("jdbc")
            .option("url", self.config["jdbc_url"])
            .option("dbtable", query)
            .option("user", self.config["jdbc_user"])
            .option("password", self.config["jdbc_password"])
            .option("driver", self.config["jdbc_driver"])
            .load()
        )
