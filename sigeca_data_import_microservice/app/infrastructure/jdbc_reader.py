from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from datetime import datetime
import re


class JDBCReader:
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
        self.spark.sparkContext.setLogLevel(config.get("log_level", "WARN"))

    def read_data(self, query) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config["jdbc_url"])
            .option("dbtable", query)
            .option("user", self.config["jdbc_user"])
            .option("password", self.config["jdbc_password"])
            .option("driver", self.config["jdbc_driver"])
            .option("encoding", "ISO-8859-1")
            .load()
        )
