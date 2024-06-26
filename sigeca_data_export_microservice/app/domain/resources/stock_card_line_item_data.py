from datetime import datetime
from uuid import uuid4

from .abstract import ResourceReader
from .util import schema_map, table_map, map_data
from ...infrastructure import ChangeLogOperation


class StockCardLineItemResourceReader(ResourceReader):
    @classmethod
    def read_table_name(cls):
        return "stock_card_line_items"

    @classmethod
    def read_schema_name(cls):
        return "stockmanagement"

    def transform_data(self, df):
        df = df.withColumn("occurreddate", df["occurreddate"].cast('string')) \
            .withColumn("processeddate", df["processeddate"].cast('string'))

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
