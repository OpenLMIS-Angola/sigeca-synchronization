from sshtunnel import SSHTunnelForwarder
from pyspark.sql import SparkSession, DataFrame
from typing import Any
import logging

logger = logging.getLogger(__name__)

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
                          - 'ssh_host': The SSH server host.
                          - 'ssh_port': The SSH server port (default is 22).
                          - 'ssh_user': The SSH username.
                          - 'ssh_private_key_path': The path to the SSH private key.
                          - 'remote_bind_address': The RDS endpoint address.
                          - 'remote_bind_port': The RDS database port.
                          - 'local_bind_port': The local port to bind the tunnel.
        """
        self.config = config
        self.spark = (
            SparkSession.builder.appName("DataSyncMicroservice")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel(config.get("log_level", "WARN"))
        self.tunnel = None

    def setup_ssh_tunnel(self):
        self.tunnel = SSHTunnelForwarder(
            (self.config["ssh_host"], self.config.get("ssh_port", 22)),
            ssh_username=self.config["ssh_user"],
            ssh_pkey=self.config["ssh_private_key_path"],
            remote_bind_address=(
                self.config["remote_bind_address"],
                self.config["remote_bind_port"],
            ),
            local_bind_address=("127.0.0.1", self.config.get("local_bind_port", 5432)),
        )
        self.tunnel.start()
        logger.info(f"SSH Tunnel established on local port {self.tunnel.local_bind_port}")

    def close_ssh_tunnel(self):
        if self.tunnel:
            self.tunnel.stop()
            self.tunnel = None
            logger.info(f"SSH Tunnel closed.")

    def read_data(self, query) -> DataFrame:
        if self.tunnel:
            local_jdbc_url = (
                self.config["jdbc_url"]
                .replace(self.config["remote_bind_address"], "127.0.0.1")
                .replace(
                    str(self.config["remote_bind_port"]),
                    str(self.config.get("local_bind_port", 5432)),
                )
            )
        else:
            local_jdbc_url = self.config["jdbc_url"]
            
        data_frame = (
            self.spark.read.format("jdbc")
            .option("url", local_jdbc_url)
            .option("dbtable", query)
            .option("user", self.config["jdbc_user"])
            .option("password", self.config["jdbc_password"])
            .option("driver", self.config["jdbc_driver"])
            .option("encoding", "ISO-8859-1")
            .load()
        )

        return data_frame
