from __future__ import annotations
import psycopg2
from typing import Any, Dict, List
from base_connector import BaseConnector, TableMetadata, TableColumnMetadata


class PostgresConnector(BaseConnector):
    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        super().__init__(host=host, port=port, database=database, user=user)
        self.conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                (schema,),
            )
            tables = cur.fetchall()
        return [
            {
                "name": t[0],
                "full_name": f"{schema}.{t[0]}",
                "table_type": "TABLE",
                "owner": None,
            }
            for t in tables
        ]

    def get_table_metadata(self, catalog: str, schema: str, table: str) -> TableMetadata:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
                """,
                (schema, table),
            )
            rows = cur.fetchall()

        columns = [
            TableColumnMetadata(
                name=r[0],
                type=r[1],
                nullable=(r[2] == "YES"),
                comment=None,
                position=r[3],
            )
            for r in rows
        ]
        return TableMetadata(
            table_name=table,
            full_name=f"{catalog}.{schema}.{table}",
            table_type="TABLE",
            data_source_format="POSTGRES",
            storage_location=None,
            owner=None,
            comment=None,
            columns=columns,
        )
