# src/graphrag/vector_indexer.py

"""
Vector Indexer for Milvus
UPDATED: Now indexes both Snowflake (OlistData) and Databricks (FederatedTable)
"""

from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()


class VectorIndexer:
    """
    Indexes table metadata into Milvus for semantic search.
    
    Now supports:
    - Snowflake tables (OlistData nodes)
    - Databricks tables (FederatedTable nodes where source='databricks')
    """
    
    def __init__(self):
        connections.connect(host='localhost', port='19530')
        print("✅ Connected to Milvus")
        
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✅ Loaded embedding model (384 dimensions)")
        
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        print("✅ Connected to Neo4j")
    
    def create_collection(self, force_recreate: bool = False):
        """Create Milvus collection with source field for filtering"""
        
        if "table_metadata" in utility.list_collections():
            if force_recreate:
                utility.drop_collection("table_metadata")
                print("🗑️ Dropped existing collection")
            else:
                print("ℹ️ Collection exists. Use force_recreate=True to rebuild.")
                return Collection("table_metadata")
        
        # Schema with source field for Snowflake/Databricks filtering
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=500, is_primary=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=2000),
            FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=50),  # NEW: snowflake/databricks
            FieldSchema(name="table_name", dtype=DataType.VARCHAR, max_length=200),  # NEW: for filtering
        ]
        
        schema = CollectionSchema(fields=fields)
        collection = Collection(name="table_metadata", schema=schema)
        print("✅ Created collection: table_metadata (with source field)")
        
        # Create HNSW index
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {"M": 16, "efConstruction": 200}
        }
        collection.create_index(field_name="embedding", index_params=index_params)
        print("✅ Created HNSW index")
        
        return collection
    
    def extract_snowflake_tables(self):
        """Get Snowflake tables from OlistData nodes"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (t:OlistData)
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:OlistColumn)
                WITH t, count(c) as col_count, collect(c.name)[..5] as sample_cols
                RETURN t.name as name, 
                       t.schema as schema,
                       t.database as database,
                       t.row_count as row_count,
                       coalesce(t.column_count, col_count) as column_count,
                       t.owner as owner,
                       sample_cols
                ORDER BY t.schema, t.name
            """)
            tables = [dict(record) for record in result]
        
        print(f"📊 Extracted {len(tables)} Snowflake tables")
        return tables
    
    def extract_databricks_tables(self):
        """Get Databricks tables from FederatedTable nodes"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (t:FederatedTable)
                WHERE t.source = 'databricks'
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:FederatedColumn)
                WITH t, count(c) as col_count, 
                     collect(c.name)[..5] as sample_cols,
                     collect(DISTINCT c.sensitivity)[..3] as sensitivities
                RETURN t.table_name as name,
                       t.full_name as full_name,
                       t.schema as schema,
                       t.row_count as row_count,
                       coalesce(t.column_count, col_count) as column_count,
                       t.owner as owner,
                       sample_cols,
                       sensitivities
                ORDER BY t.full_name
            """)
            tables = [dict(record) for record in result]
        
        print(f"🧱 Extracted {len(tables)} Databricks tables")
        return tables
    
    def _create_rich_text(self, table: dict, source: str) -> str:
        """
        Create rich text representation for embedding.
        Includes table name, schema, row count, owner, and sample columns.
        """
        if source == 'snowflake':
            text_parts = [
                f"{table['schema']}.{table['name']}",
                f"({table.get('row_count', 0):,} rows)",
                f"[Snowflake]"
            ]
            if table.get('owner'):
                text_parts.append(f"Owner: {table['owner']}")
            if table.get('sample_cols'):
                cols = ', '.join(table['sample_cols'][:3])
                text_parts.append(f"Columns: {cols}")
        else:  # databricks
            text_parts = [
                f"{table['full_name']}",
                f"({table.get('row_count', 0):,} rows)",
                f"[Databricks]"
            ]
            if table.get('owner'):
                text_parts.append(f"Owner: {table['owner']}")
            if table.get('sample_cols'):
                cols = ', '.join(table['sample_cols'][:3])
                text_parts.append(f"Columns: {cols}")
            if table.get('sensitivities'):
                sens = ', '.join([s for s in table['sensitivities'] if s])
                if sens:
                    text_parts.append(f"Sensitivity: {sens}")
        
        return ' | '.join(text_parts)
    
    def index_all_tables(self, force_recreate: bool = True):
        """Index both Snowflake and Databricks tables into Milvus"""
        
        print("\n🚀 Starting unified vector indexing pipeline...")
        print("=" * 60)
        
        collection = self.create_collection(force_recreate=force_recreate)
        
        # Extract from both sources
        snowflake_tables = self.extract_snowflake_tables()
        databricks_tables = self.extract_databricks_tables()
        
        total = len(snowflake_tables) + len(databricks_tables)
        print(f"\n📦 Total tables to index: {total}")
        print("-" * 60)
        
        # Index Snowflake tables
        print("\n❄️ Indexing Snowflake tables...")
        for i, table in enumerate(snowflake_tables, 1):
            text = self._create_rich_text(table, 'snowflake')
            embedding = self.model.encode(text)
            table_id = f"snowflake.{table['schema']}.{table['name']}".lower()
            
            try:
                collection.insert([
                    [table_id],
                    [embedding.tolist()],
                    [text],
                    ['snowflake'],
                    [table['name']]
                ])
                print(f"  ✓ [{i}/{len(snowflake_tables)}] {table['schema']}.{table['name']}")
            except Exception as e:
                print(f"  ✗ [{i}/{len(snowflake_tables)}] {table['schema']}.{table['name']} - {e}")
        
        # Index Databricks tables
        print("\n🧱 Indexing Databricks tables...")
        for i, table in enumerate(databricks_tables, 1):
            text = self._create_rich_text(table, 'databricks')
            embedding = self.model.encode(text)
            table_id = f"databricks.{table['full_name']}".lower()
            
            try:
                collection.insert([
                    [table_id],
                    [embedding.tolist()],
                    [text],
                    ['databricks'],
                    [table['name']]
                ])
                print(f"  ✓ [{i}/{len(databricks_tables)}] {table['full_name']}")
            except Exception as e:
                print(f"  ✗ [{i}/{len(databricks_tables)}] {table['full_name']} - {e}")
        
        collection.flush()
        print(f"\n✅ Indexing complete: {total} tables")
        
        collection.load()
        print("✅ Collection loaded and ready for search")
        
        return collection
    
    def search(self, query_text: str, top_k: int = 5, source_filter: str = None):
        """
        Semantic search with optional source filtering.
        
        Args:
            query_text: Search query
            top_k: Number of results
            source_filter: 'snowflake', 'databricks', or None for both
        """
        collection = Collection("table_metadata")
        collection.load()
        
        query_embedding = self.model.encode(query_text).tolist()
        
        # Build filter expression
        expr = None
        if source_filter:
            expr = f'source == "{source_filter}"'
        
        results = collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k,
            output_fields=["text", "source", "table_name"],
            expr=expr
        )
        
        return results[0]
    
    def test_search(self, query_text: str = "customer", source_filter: str = None):
        """Test semantic search"""
        
        filter_label = f" (filter: {source_filter})" if source_filter else " (all sources)"
        print(f"\n🔍 Search: '{query_text}'{filter_label}")
        print("=" * 60)
        
        results = self.search(query_text, top_k=5, source_filter=source_filter)
        
        print(f"\n📋 Top 5 Results:\n")
        for i, hit in enumerate(results, 1):
            source_icon = "❄️" if hit.entity.get('source') == 'snowflake' else "🧱"
            print(f"{i}. {source_icon} {hit.entity.get('text')}")
            print(f"   Similarity: {hit.distance:.3f}\n")
        
        return results
    
    def get_stats(self) -> dict:
        """Get collection statistics"""
        collection = Collection("table_metadata")
        
        stats = {
            'total_entities': collection.num_entities,
        }
        
        # Count by source
        collection.load()
        
        # Note: Milvus doesn't have easy count with filter, 
        # so we'll query with high limit
        sf_results = self.search("table", top_k=100, source_filter='snowflake')
        db_results = self.search("table", top_k=100, source_filter='databricks')
        
        stats['snowflake_tables'] = len(sf_results)
        stats['databricks_tables'] = len(db_results)
        
        return stats
    
    def close(self):
        self.driver.close()
        connections.disconnect("default")
        print("✅ Connections closed")


# ============================================
# CLI
# ============================================

if __name__ == "__main__":
    import sys
    
    indexer = VectorIndexer()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--rebuild":
        # Full rebuild
        indexer.index_all_tables(force_recreate=True)
    else:
        print("\nUsage: python vector_indexer.py --rebuild")
        print("\nRunning test searches instead...\n")
    
    # Test searches
    print("\n" + "=" * 60)
    print("🧪 TEST SEARCHES")
    print("=" * 60)
    
    indexer.test_search("customer data")
    indexer.test_search("sales transactions", source_filter='databricks')
    indexer.test_search("feedback", source_filter='databricks')
    indexer.test_search("orders", source_filter='snowflake')
    
    # Stats
    print("\n📊 Collection Stats:")
    stats = indexer.get_stats()
    for k, v in stats.items():
        print(f"   {k}: {v}")
    
    indexer.close()