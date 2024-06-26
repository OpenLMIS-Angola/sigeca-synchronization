from datetime import datetime
from uuid import uuid4

from app.domain.resources.abstract import ResourceReader
from app.domain.resources.util import table_map, schema_map, map_data
from app.infrastructure import ChangeLogOperation


class ProgramResourceReader(ResourceReader):
    pass

    @classmethod
    def read_table_name(cls):
        return "programs"

    @classmethod
    def read_schema_name(cls):
        return "referencedata"

    def transform_data(self, df):
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
