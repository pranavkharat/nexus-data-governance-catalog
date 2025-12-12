from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

@dataclass
class TableColumnMetadata:
    name: str
    type: str
    nullable: bool
    comment: Optional[str]
    position: int


@dataclass
class TableMetadata:
    table_name: str
    full_name: str
    table_type: Optional[str]
    data_source_format: Optional[str]
    storage_location: Optional[str]
    owner: Optional[str]
    comment: Optional[str]
    columns: List[TableColumnMetadata]


class DatabricksTableInspector:
    """Encapsulates Databricks interactions for table inspection.

    Authentication is taken from explicit host/token if provided, otherwise
    falls back to Databricks SDK's default resolution (env/config).
    """

    def __init__(self, host: Optional[str] = None, token: Optional[str] = None) -> None:
        if (host is None) ^ (token is None):
            raise ValueError("Both --host and --token must be provided together, or neither.")
        if host and token:
            self._client = WorkspaceClient(host=host, token=token)
        else:
            self._client = WorkspaceClient()

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        tables = self._client.tables.list(catalog_name=catalog, schema_name=schema)
        results: List[Dict[str, Any]] = []
        for tbl in tables:
            results.append(
                {
                    "name": tbl.name,
                    "full_name": tbl.full_name,
                    "table_type": str(tbl.table_type) if tbl.table_type is not None else None,
                    "owner": tbl.owner,
                }
            )
        return results

    def get_table_metadata(self, catalog: str, schema: str, table: str) -> TableMetadata:
        full_name = f"{catalog}.{schema}.{table}"
        info = self._client.tables.get(full_name=full_name)
        columns = [
            TableColumnMetadata(
                name=col.name,
                type=str(col.type_name) if col.type_name is not None else "",
                nullable=bool(col.nullable),
                comment=col.comment,
                position=int(col.position),
            )
            for col in info.columns
        ]
        return TableMetadata(
            table_name=info.name,
            full_name=info.full_name,
            table_type=str(info.table_type) if info.table_type is not None else None,
            data_source_format=str(info.data_source_format) if info.data_source_format is not None else None,
            storage_location=info.storage_location,
            owner=info.owner,
            comment=info.comment,
            columns=columns,
        )

    def export_table_metadata(self, metadata: TableMetadata, output_path: str) -> None:
        data = asdict(metadata)
        # Ensure output directory exists
        output_dir = os.path.dirname(os.path.abspath(output_path))
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


def _print_table_overview(tables: List[Dict[str, Any]]) -> None:
    if not tables:
        print("No tables found.")
        return
    for t in tables:
        print(f"Table: {t['full_name']}")
        print(f"  Type: {t['table_type']}")
        print(f"  Owner: {t['owner']}")
        print()


def _print_table_metadata(metadata: TableMetadata) -> None:
    print(f"Table Name: {metadata.table_name}")
    print(f"Table Type: {metadata.table_type}")
    print(f"Data Source Format: {metadata.data_source_format}")
    print(f"Storage Location: {metadata.storage_location}")
    print(f"Owner: {metadata.owner}")
    print(f"Comment: {metadata.comment}")
    print("\nColumns:")
    print("-" * 80)
    for col in metadata.columns:
        print(f"Column: {col.name}")
        print(f"  Type: {col.type}")
        print(f"  Nullable: {col.nullable}")
        print(f"  Comment: {col.comment}")
        print(f"  Position: {col.position}")
        print()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect and export Databricks Unity Catalog table metadata",
    )
    parser.add_argument(
        "--host",
        help="Databricks workspace host (also reads DATABRICKS_HOST)",
        default=os.getenv("DATABRICKS_HOST"),
    )
    parser.add_argument(
        "--token",
        help="Databricks personal access token (also reads DATABRICKS_TOKEN)",
        default=os.getenv("DATABRICKS_TOKEN"),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_list = subparsers.add_parser("list-tables", help="List tables in a catalog.schema")
    p_list.add_argument("--catalog", required=True, help="Catalog name")
    p_list.add_argument("--schema", required=True, help="Schema name")

    p_desc = subparsers.add_parser("describe-table", help="Describe a table's metadata")
    p_desc.add_argument("--catalog", required=True, help="Catalog name")
    p_desc.add_argument("--schema", required=True, help="Schema name")
    p_desc.add_argument("--table", required=True, help="Table name")

    p_export = subparsers.add_parser("export-metadata", help="Export table metadata to JSON")
    p_export.add_argument("--catalog", required=True, help="Catalog name")
    p_export.add_argument("--schema", required=True, help="Schema name")
    p_export.add_argument("--table", required=True, help="Table name")
    p_export.add_argument(
        "--out",
        default="table_metadata.json",
        help="Output JSON file path (default: table_metadata.json)",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        inspector = DatabricksTableInspector(host=args.host, token=args.token)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Failed to initialize Databricks client: {e}", file=sys.stderr)
        return 2

    try:
        if args.command == "list-tables":
            tables = inspector.list_tables(catalog=args.catalog, schema=args.schema)
            _print_table_overview(tables)
            return 0

        if args.command == "describe-table":
            metadata = inspector.get_table_metadata(
                catalog=args.catalog, schema=args.schema, table=args.table
            )
            _print_table_metadata(metadata)
            return 0

        if args.command == "export-metadata":
            metadata = inspector.get_table_metadata(
                catalog=args.catalog, schema=args.schema, table=args.table
            )
            inspector.export_table_metadata(metadata, output_path=args.out)
            print(f"Metadata exported to {os.path.abspath(args.out)}")
            return 0
    except Exception as e:
        print(f"Operation failed: {e}", file=sys.stderr)
        return 1

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())