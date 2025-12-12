from __future__ import annotations
import abc
import json
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


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


class BaseConnector(abc.ABC):
    """Abstract base class for all database connectors."""

    def __init__(self, **kwargs: Any) -> None:
        self.config = kwargs

    @abc.abstractmethod
    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        pass

    @abc.abstractmethod
    def get_table_metadata(self, catalog: str, schema: str, table: str) -> TableMetadata:
        pass

    def export_table_metadata(self, metadata: TableMetadata, output_path: str) -> None:
        """Export metadata to JSON."""
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(asdict(metadata), f, indent=2)
        
