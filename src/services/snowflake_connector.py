from __future__ import annotations
import snowflake.connector
from typing import Any, Dict, List, Optional
from base_connector import BaseConnector, TableMetadata, TableColumnMetadata


class SnowflakeConnector(BaseConnector):
    def __init__(self, account: str, user: str, password: str, warehouse: str, database: str) -> None:
        super().__init__(account=account, user=user, warehouse=warehouse, database=database)
        self.conn = snowflake.connector.connect(
            account=account, user=user, password=password, warehouse=warehouse, database=database
        )

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
        results = [
            {
                "name": row[1],
                "full_name": f"{catalog}.{schema}.{row[1]}",
                "table_type": "TABLE",
                "owner": row[3],
            }
            for row in cursor.fetchall()
        ]
        cursor.close()
        return results

    def get_table_metadata(self, catalog: str, schema: str, table: str) -> TableMetadata:
        cursor = self.conn.cursor()
        cursor.execute(f"DESC TABLE {catalog}.{schema}.{table}")
        columns = [
            TableColumnMetadata(
                name=row[0],
                type=row[1],
                nullable=row[3] == "Y",
                comment=row[6],
                position=i + 1,
            )
            for i, row in enumerate(cursor.fetchall())
        ]
        cursor.close()
        return TableMetadata(
            table_name=table,
            full_name=f"{catalog}.{schema}.{table}",
            table_type="TABLE",
            data_source_format="SNOWFLAKE",
            storage_location=None,
            owner=None,
            comment=None,
            columns=columns,
        )
