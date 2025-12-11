# src/federation/databricks_metadata_extractor.py

"""
Databricks Metadata Extractor for Federated Knowledge Graph
Extracts metadata fingerprints WITHOUT exposing raw data.

Privacy-Preserving Design:
- Only extracts structural metadata (table/column names, types, counts)
- NO raw data values are ever transferred
- Fingerprints use hashed signatures for cross-source matching
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ColumnFingerprint:
    """Privacy-preserving column metadata"""
    name: str
    data_type: str
    position: int
    nullable: bool = True
    comment: Optional[str] = None
    sensitivity: str = "Low"  # Low, Medium, High
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'data_type': self.data_type,
            'position': self.position,
            'nullable': self.nullable,
            'comment': self.comment,
            'sensitivity': self.sensitivity
        }


@dataclass
class TableFingerprint:
    """Privacy-preserving table fingerprint for federation"""
    source: str  # 'databricks'
    catalog: str
    schema: str
    table_name: str
    full_name: str
    
    # Structural fingerprint (safe to share)
    row_count: int = 0
    column_count: int = 0
    columns: List[ColumnFingerprint] = field(default_factory=list)
    
    # Ownership & governance
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    comment: Optional[str] = None
    
    # Computed fingerprints (hashed, privacy-safe)
    column_signature: str = ""  # Hash of column names
    type_signature: str = ""    # Hash of data types
    
    # Metadata
    extracted_at: datetime = field(default_factory=datetime.now)
    
    def compute_signatures(self):
        """Compute hashed signatures for privacy-safe matching"""
        # Column name signature (sorted for consistency)
        col_names = sorted([c.name.lower() for c in self.columns])
        self.column_signature = hashlib.sha256(
            '|'.join(col_names).encode()
        ).hexdigest()[:16]
        
        # Type signature
        type_list = sorted([c.data_type.upper() for c in self.columns])
        self.type_signature = hashlib.sha256(
            '|'.join(type_list).encode()
        ).hexdigest()[:16]
    
    def to_dict(self) -> Dict:
        return {
            'source': self.source,
            'catalog': self.catalog,
            'schema': self.schema,
            'table_name': self.table_name,
            'full_name': self.full_name,
            'row_count': self.row_count,
            'column_count': self.column_count,
            'columns': [c.to_dict() for c in self.columns],
            'owner': self.owner,
            'tags': self.tags,
            'comment': self.comment,
            'column_signature': self.column_signature,
            'type_signature': self.type_signature,
            'extracted_at': self.extracted_at.isoformat()
        }


class DatabricksMetadataExtractor:
    """
    Extracts metadata from Databricks Unity Catalog.
    
    Privacy-Preserving:
    - Only structural metadata is extracted
    - No raw data values are ever accessed
    - Fingerprints enable cross-source matching without data exposure
    """
    
    def __init__(self, host: str = None, token: str = None, warehouse_id: str = None):
        """
        Initialize Databricks connection.
        
        Args:
            host: Databricks workspace URL
            token: Personal access token
            warehouse_id: SQL warehouse ID for queries
        """
        self.host = host or os.getenv('DATABRICKS_HOST')
        self.token = token or os.getenv('DATABRICKS_TOKEN')
        self.warehouse_id = warehouse_id or os.getenv('DATABRICKS_WAREHOUSE_ID')
        
        if not all([self.host, self.token]):
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN required")
        
        self.client = WorkspaceClient(host=self.host, token=self.token)
        print(f"✅ Connected to Databricks: {self.host}")
    
    def list_catalogs(self) -> List[str]:
        """List available catalogs"""
        catalogs = list(self.client.catalogs.list())
        return [cat.name for cat in catalogs]
    
    def list_schemas(self, catalog: str) -> List[str]:
        """List schemas in a catalog"""
        schemas = list(self.client.schemas.list(catalog_name=catalog))
        return [s.name for s in schemas]
    
    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """List tables in a schema"""
        tables = self.client.tables.list(catalog_name=catalog, schema_name=schema)
        return [
            {
                'name': tbl.name,
                'full_name': tbl.full_name,
                'table_type': str(tbl.table_type) if tbl.table_type else None,
                'owner': tbl.owner
            }
            for tbl in tables
        ]
    
    def extract_table_fingerprint(self, catalog: str, schema: str, 
                                   table: str) -> TableFingerprint:
        """
        Extract privacy-preserving fingerprint for a single table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            TableFingerprint with metadata (no raw data)
        """
        full_name = f"{catalog}.{schema}.{table}"
        
        # Get table info from Unity Catalog
        table_info = self.client.tables.get(full_name=full_name)
        
        # Extract column metadata
        columns = []
        for col in table_info.columns:
            col_fp = ColumnFingerprint(
                name=col.name,
                data_type=str(col.type_name) if col.type_name else 'UNKNOWN',
                position=col.position if col.position else 0,
                nullable=col.nullable if col.nullable is not None else True,
                comment=col.comment
            )
            columns.append(col_fp)
        
        # Get row count (if warehouse available)
        row_count = self._get_row_count(full_name)
        
        # Parse owner/tags from comment if structured
        owner, tags = self._parse_ownership(table_info.comment)
        
        # Create fingerprint
        fingerprint = TableFingerprint(
            source='databricks',
            catalog=catalog,
            schema=schema,
            table_name=table,
            full_name=f"databricks.{full_name}",
            row_count=row_count,
            column_count=len(columns),
            columns=columns,
            owner=owner or table_info.owner,
            tags=tags,
            comment=table_info.comment
        )
        
        # Compute privacy-safe signatures
        fingerprint.compute_signatures()
        
        return fingerprint
    
    def _get_row_count(self, full_name: str) -> int:
        """Get approximate row count using SQL warehouse"""
        if not self.warehouse_id:
            return 0
        
        try:
            result = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"SELECT COUNT(*) as cnt FROM {full_name}",
                wait_timeout="30s"
            )
            
            if result.status.state == sql.StatementState.SUCCEEDED:
                if result.result and result.result.data_array:
                    return int(result.result.data_array[0][0])
        except Exception as e:
            print(f"   ⚠️ Could not get row count: {e}")
        
        return 0
    
    def _parse_ownership(self, comment: str) -> tuple:
        """Parse owner and tags from table comment"""
        owner = None
        tags = []
        
        if comment:
            # Look for "Owner: X" pattern
            if 'Owner:' in comment:
                parts = comment.split('Owner:')
                if len(parts) > 1:
                    owner_part = parts[1].split('|')[0].strip()
                    owner = owner_part
            
            # Look for "Tags: X, Y, Z" pattern
            if 'Tags:' in comment:
                parts = comment.split('Tags:')
                if len(parts) > 1:
                    tags_part = parts[1].split('|')[0].strip()
                    tags = [t.strip() for t in tags_part.split(',')]
        
        return owner, tags
    
    def extract_all_fingerprints(self, catalog: str, schema: str) -> List[TableFingerprint]:
        """
        Extract fingerprints for all tables in a schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of TableFingerprints
        """
        print(f"\n{'='*70}")
        print(f"📊 EXTRACTING DATABRICKS METADATA FINGERPRINTS")
        print(f"   Catalog: {catalog}")
        print(f"   Schema: {schema}")
        print(f"{'='*70}")
        
        fingerprints = []
        tables = self.list_tables(catalog, schema)
        
        for table_info in tables:
            table_name = table_info['name']
            
            # Skip metadata catalog itself
            if table_name == 'metadata_catalog':
                continue
            
            print(f"\n   📋 Extracting: {table_name}")
            
            try:
                fp = self.extract_table_fingerprint(catalog, schema, table_name)
                fingerprints.append(fp)
                print(f"      ✅ {fp.column_count} columns, {fp.row_count:,} rows")
                print(f"      🔐 Column signature: {fp.column_signature}")
            except Exception as e:
                print(f"      ❌ Error: {e}")
        
        print(f"\n{'='*70}")
        print(f"✅ Extracted {len(fingerprints)} table fingerprints")
        print(f"{'='*70}")
        
        return fingerprints


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🧪 TESTING DATABRICKS METADATA EXTRACTOR")
    print("="*70)
    
    extractor = DatabricksMetadataExtractor()
    
    # List catalogs
    print("\n📚 Available Catalogs:")
    catalogs = extractor.list_catalogs()
    for cat in catalogs:
        print(f"   - {cat}")
    
    # Extract fingerprints from sample_data
    CATALOG = "workspace"
    SCHEMA = "sample_data"
    
    fingerprints = extractor.extract_all_fingerprints(CATALOG, SCHEMA)
    
    # Display results
    print("\n📊 EXTRACTED FINGERPRINTS:")
    print("-"*70)
    
    for fp in fingerprints:
        print(f"\n📋 {fp.full_name}")
        print(f"   Rows: {fp.row_count:,}")
        print(f"   Columns: {fp.column_count}")
        print(f"   Owner: {fp.owner}")
        print(f"   Tags: {fp.tags}")
        print(f"   Column Signature: {fp.column_signature}")
        print(f"   Type Signature: {fp.type_signature}")
        print(f"   Columns:")
        for col in fp.columns[:5]:  # Show first 5
            print(f"      - {col.name} ({col.data_type})")
        if len(fp.columns) > 5:
            print(f"      ... and {len(fp.columns) - 5} more")
    
    print("\n✅ Testing complete!")