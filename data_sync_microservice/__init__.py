import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import json 
from app.infrastructure import JDBCReader
from app.application import FacilityResourceSynchronization
from datetime import datetime
from app.infrastructure import ChangeLogOperationEnum

def load_config(file_path='./config.json'):
    with open(file_path, 'r') as file:
        return json.load(file)


def main():
    config = load_config()
    jdbc_reader = JDBCReader(config)

    sync = FacilityResourceSynchronization(jdbc_reader)
    sync.execute_full_synchronization()
    sync.execute_change_synchronization(ChangeLogOperationEnum.UPDATE, datetime(2019, 1, 1))
    jdbc_reader.spark.stop()

if __name__ == "__main__":
    main()
