from __future__ import annotations
from databricks.sdk import WorkspaceClient
from typing import Any, Dict, List, Optional
from base_connector import BaseConnector, TableColumnMetadata, TableMetadata


class DatabricksConnector(BaseConnector):
    def __init__(self, host: Optional[str] = None, token: Optional[str] = None) -> None:
        super().__init__(host=host, token=token)
        if (host is None) ^ (token is None):
            raise ValueError("Both host and token must be provided together.")
        self._client = WorkspaceClient(host=host, token=token) if host else WorkspaceClient()

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        tables = self._client.tables.list(catalog_name=catalog, schema_name=schema)
        return [
            {
                "name": tbl.name,
                "full_name": tbl.full_name,
                "table_type": str(tbl.table_type),
                "owner": tbl.owner,
            }
            for tbl in tables
        ]

    def get_table_metadata(self, catalog: str, schema: str, table: str) -> TableMetadata:
        info = self._client.tables.get(full_name=f"{catalog}.{schema}.{table}")
        columns = [
            TableColumnMetadata(
                name=col.name,
                type=str(col.type_name),
                nullable=bool(col.nullable),
                comment=col.comment,
                position=int(col.position),
            )
            for col in info.columns
        ]
        return TableMetadata(
            table_name=info.name,
            full_name=info.full_name,
            table_type=str(info.table_type),
            data_source_format=str(info.data_source_format),
            storage_location=info.storage_location,
            owner=info.owner,
            comment=info.comment,
            columns=columns,
        )
