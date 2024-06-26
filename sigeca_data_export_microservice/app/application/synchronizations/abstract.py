from abc import ABC, abstractmethod
from typing import Type

from app.domain.resources.abstract import ResourceReader
from app.infrastructure import JDBCReader, SigecaApiClient
import logging


class ResourceSynchronization(ABC):
    @property
    @abstractmethod
    def synchronized_resource(self) -> Type[ResourceReader]:
        pass

    @classmethod
    def get_resource_name(cls):
        return f"{cls.synchronized_resource.read_schema_name()}.{cls.synchronized_resource.read_table_name()}"

    def __init__(self, jdbc_reader: JDBCReader, api_client: SigecaApiClient):
        self.jdbc_reader = jdbc_reader
        self.resource = self.synchronized_resource(jdbc_reader)
        self.api_client = api_client

    def execute_full_synchronization(self):
        data = self.resource.get_all_data()
        self.synchronize_data(data)

    def execute_change_synchronization(self):
        data = self.resource.get_changelog_data()
        self.synchronize_data(data)

    def synchronize_data(self, payload):
        count = len(payload)

        if count:
            logging.info(f"Syncing {count} records for {self.get_resource_name()}")
            self.api_client.sync(payload)
        else:
            logging.info(f"No records to sync for {self.get_resource_name()}")

    def __str__(self) -> str:
        return self.__class__.__name__
