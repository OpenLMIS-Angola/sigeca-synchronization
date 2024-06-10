import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import json 
from app.infrastructure import JDBCReader
from app.application import DataSyncService
from app.application.synchronizations import FacilityResourceSynchronization
from datetime import datetime
from app.infrastructure import ChangeLogOperationEnum
from app.infrastructure import ResourceAPIClient

def load_config(file_path='./config.json'):
    with open(file_path, 'r') as file:
        return json.load(file)


def main():
    config = load_config()
    jdbc_reader = JDBCReader(config)
    api_client = ResourceAPIClient(config['api']['url'], config['api']['token'])

    sync = DataSyncService(jdbc_reader, api_client)
    sync.sync_full(FacilityResourceSynchronization)
    sync.sync_change(FacilityResourceSynchronization, ChangeLogOperationEnum.UPDATE, datetime(2019, 1, 1))
    jdbc_reader.spark.stop()

if __name__ == "__main__":
    main()
