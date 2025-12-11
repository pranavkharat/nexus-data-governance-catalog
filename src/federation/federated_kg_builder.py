# src/federation/federated_kg_builder.py

"""
Federated Knowledge Graph Builder
Creates a unified Neo4j graph with tables from multiple sources.

Design Principles:
1. Source Attribution: Every node tagged with origin (snowflake/databricks)
2. Privacy-Preserving: Only fingerprints stored, no raw data
3. Cross-Source Discovery: Enable finding similar tables across sources
4. Unified Schema: Single node type (FederatedTable) for all sources
"""

from neo4j import GraphDatabase
from typing import List, Dict, Optional
from datetime import datetime
import os
from dotenv import load_dotenv

# Import fingerprint types
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.federation.databricks_metadata_extractor import (
    DatabricksMetadataExtractor, 
    TableFingerprint
)

load_dotenv()


class FederatedKGBuilder:
    """
    Builds a unified Knowledge Graph from multiple data sources.
    
    Node Types:
    - FederatedTable: Unified table representation
    - FederatedColumn: Column metadata
    - DataSource: Source system (snowflake, databricks)
    - Team: Ownership
    
    Relationships:
    - [:FROM_SOURCE] - Table to DataSource
    - [:HAS_COLUMN] - Table to Column
    - [:OWNED_BY] - Table to Team
    - [:SIMILAR_TO] - Cross-source similarity (fingerprint-based)
    """
    
    def __init__(self):
        """Initialize Neo4j connection"""
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        
        # Verify connection
        with self.driver.session() as session:
            session.run("RETURN 1")
        
        print("✅ Connected to Neo4j for federated graph building")
        
        # Statistics
        self.stats = {
            'tables_created': 0,
            'columns_created': 0,
            'sources_created': 0,
            'similarities_found': 0
        }
    
    def create_constraints(self):
        """Create uniqueness constraints for federated schema"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:FederatedTable) REQUIRE t.full_name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:FederatedColumn) REQUIRE c.full_name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:DataSource) REQUIRE s.name IS UNIQUE",
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    pass  # Constraint may already exist
        
        print("✅ Federated constraints created")
    
    def create_data_source(self, source_name: str, source_type: str, 
                           connection_info: Dict = None):
        """
        Create a DataSource node representing a data platform.
        
        Args:
            source_name: Unique source identifier (e.g., 'snowflake_olist')
            source_type: Platform type ('snowflake', 'databricks')
            connection_info: Non-sensitive connection metadata
        """
        with self.driver.session() as session:
            session.run("""
                MERGE (s:DataSource {name: $name})
                SET s.type = $type,
                    s.created_at = datetime(),
                    s.table_count = 0
            """, name=source_name, type=source_type)
        
        self.stats['sources_created'] += 1
        print(f"✅ Created DataSource: {source_name} ({source_type})")
    
    def add_databricks_tables(self, fingerprints: List[TableFingerprint], 
                               source_name: str = 'databricks_workspace'):
        """
        Add Databricks tables to the federated graph.
        
        Args:
            fingerprints: List of TableFingerprint from extractor
            source_name: Source identifier
        """
        print(f"\n{'='*70}")
        print(f"📥 ADDING DATABRICKS TABLES TO FEDERATED GRAPH")
        print(f"{'='*70}")
        
        # Ensure source exists
        self.create_data_source(source_name, 'databricks')
        
        with self.driver.session() as session:
            for fp in fingerprints:
                # Create FederatedTable node
                session.run("""
                    MERGE (t:FederatedTable {full_name: $full_name})
                    SET t.source = 'databricks',
                        t.source_name = $source_name,
                        t.catalog = $catalog,
                        t.schema = $schema,
                        t.table_name = $table_name,
                        t.row_count = $row_count,
                        t.column_count = $column_count,
                        t.owner = $owner,
                        t.tags = $tags,
                        t.column_signature = $column_signature,
                        t.type_signature = $type_signature,
                        t.created_at = datetime()
                    
                    WITH t
                    MATCH (s:DataSource {name: $source_name})
                    MERGE (t)-[:FROM_SOURCE]->(s)
                """,
                    full_name=fp.full_name,
                    source_name=source_name,
                    catalog=fp.catalog,
                    schema=fp.schema,
                    table_name=fp.table_name,
                    row_count=fp.row_count,
                    column_count=fp.column_count,
                    owner=fp.owner,
                    tags=fp.tags,
                    column_signature=fp.column_signature,
                    type_signature=fp.type_signature
                )
                
                self.stats['tables_created'] += 1
                print(f"   ✅ Added: {fp.full_name}")
                
                # Create FederatedColumn nodes
                for col in fp.columns:
                    col_full_name = f"{fp.full_name}.{col.name}"
                    
                    session.run("""
                        MERGE (c:FederatedColumn {full_name: $col_full_name})
                        SET c.name = $col_name,
                            c.data_type = $data_type,
                            c.position = $position,
                            c.nullable = $nullable,
                            c.sensitivity = $sensitivity,
                            c.source = 'databricks'
                        
                        WITH c
                        MATCH (t:FederatedTable {full_name: $table_full_name})
                        MERGE (t)-[:HAS_COLUMN]->(c)
                    """,
                        col_full_name=col_full_name,
                        col_name=col.name,
                        data_type=col.data_type,
                        position=col.position,
                        nullable=col.nullable,
                        sensitivity=col.sensitivity,
                        table_full_name=fp.full_name
                    )
                    
                    self.stats['columns_created'] += 1
        
        # Update source table count
        with self.driver.session() as session:
            session.run("""
                MATCH (s:DataSource {name: $source_name})
                SET s.table_count = $count
            """, source_name=source_name, count=len(fingerprints))
        
        print(f"\n✅ Added {len(fingerprints)} Databricks tables to federated graph")
    
    def add_snowflake_tables_as_federated(self, source_name: str = 'snowflake_olist'):
        """
        Convert existing OlistData nodes to FederatedTable nodes.
        Preserves original nodes, creates federated view.
        """
        print(f"\n{'='*70}")
        print(f"📥 ADDING SNOWFLAKE TABLES TO FEDERATED GRAPH")
        print(f"{'='*70}")
        
        # Create source
        self.create_data_source(source_name, 'snowflake')
        
        with self.driver.session() as session:
            # Get all OlistData nodes and create FederatedTable equivalents
            result = session.run("""
                MATCH (od:OlistData)
                RETURN od.schema as schema, od.name as name, 
                       od.row_count as row_count, od.column_count as column_count,
                       od.fingerprint as fingerprint
            """)
            
            tables = list(result)
            
            for record in tables:
                full_name = f"snowflake.{record['schema']}.{record['name']}"
                
                # Create FederatedTable linked to original OlistData
                session.run("""
                    MERGE (t:FederatedTable {full_name: $full_name})
                    SET t.source = 'snowflake',
                        t.source_name = $source_name,
                        t.catalog = 'TRAINING_DB',
                        t.schema = $schema,
                        t.table_name = $table_name,
                        t.row_count = $row_count,
                        t.column_count = $column_count,
                        t.fingerprint = $fingerprint,
                        t.created_at = datetime()
                    
                    WITH t
                    MATCH (s:DataSource {name: $source_name})
                    MERGE (t)-[:FROM_SOURCE]->(s)
                    
                    WITH t
                    MATCH (od:OlistData {name: $table_name, schema: $schema})
                    MERGE (t)-[:MIRRORS]->(od)
                """,
                    full_name=full_name,
                    source_name=source_name,
                    schema=record['schema'],
                    table_name=record['name'],
                    row_count=record['row_count'] or 0,
                    column_count=record['column_count'] or 0,
                    fingerprint=record['fingerprint']
                )
                
                self.stats['tables_created'] += 1
                print(f"   ✅ Added: {full_name}")
            
            # Update source count
            session.run("""
                MATCH (s:DataSource {name: $source_name})
                SET s.table_count = $count
            """, source_name=source_name, count=len(tables))
        
        print(f"\n✅ Added {len(tables)} Snowflake tables to federated graph")
    
    def compute_cross_source_similarities(self, threshold: float = 0.5):
        """
        Find similar tables across different sources using fingerprints.
        Creates [:SIMILAR_TO] relationships.
        
        Privacy-Safe: Only compares structural signatures, not actual data.
        """
        print(f"\n{'='*70}")
        print(f"🔍 COMPUTING CROSS-SOURCE SIMILARITIES")
        print(f"{'='*70}")
        
        with self.driver.session() as session:
            # Find tables with matching column signatures across sources
            result = session.run("""
                MATCH (t1:FederatedTable), (t2:FederatedTable)
                WHERE t1.source <> t2.source
                  AND t1.full_name < t2.full_name
                  AND (
                      t1.column_signature = t2.column_signature
                      OR t1.type_signature = t2.type_signature
                      OR abs(t1.column_count - t2.column_count) <= 2
                  )
                WITH t1, t2,
                     CASE WHEN t1.column_signature = t2.column_signature THEN 0.5 ELSE 0 END +
                     CASE WHEN t1.type_signature = t2.type_signature THEN 0.3 ELSE 0 END +
                     CASE WHEN abs(t1.column_count - t2.column_count) <= 2 THEN 0.2 ELSE 0 END
                     AS similarity
                WHERE similarity >= $threshold
                MERGE (t1)-[r:SIMILAR_TO]-(t2)
                SET r.similarity = similarity,
                    r.match_type = CASE 
                        WHEN t1.column_signature = t2.column_signature THEN 'COLUMN_SIGNATURE'
                        WHEN t1.type_signature = t2.type_signature THEN 'TYPE_SIGNATURE'
                        ELSE 'STRUCTURE'
                    END,
                    r.computed_at = datetime()
                RETURN t1.full_name as table1, t2.full_name as table2, 
                       similarity, r.match_type as match_type
            """, threshold=threshold)
            
            similarities = list(result)
            
            for sim in similarities:
                self.stats['similarities_found'] += 1
                print(f"   🔗 {sim['table1']}")
                print(f"      ↔️ {sim['table2']}")
                print(f"      Similarity: {sim['similarity']:.0%} ({sim['match_type']})")
        
        print(f"\n✅ Found {self.stats['similarities_found']} cross-source similarities")
    
    def get_federated_statistics(self) -> Dict:
        """Get statistics about the federated graph"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (t:FederatedTable)
                WITH t.source as source, count(t) as tables
                RETURN collect({source: source, tables: tables}) as by_source
            """)
            by_source = result.single()['by_source']
            
            result = session.run("""
                MATCH (t:FederatedTable)
                OPTIONAL MATCH (t)-[s:SIMILAR_TO]-()
                RETURN count(DISTINCT t) as total_tables,
                       count(DISTINCT s) as cross_source_links
            """)
            stats = result.single()
            
            return {
                'total_federated_tables': stats['total_tables'],
                'cross_source_similarities': stats['cross_source_links'],
                'by_source': {item['source']: item['tables'] for item in by_source}
            }
    
    def get_all_federated_tables(self) -> List[Dict]:
        """Get all tables in the federated graph"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (t:FederatedTable)-[:FROM_SOURCE]->(s:DataSource)
                OPTIONAL MATCH (t)-[sim:SIMILAR_TO]-()
                RETURN t.full_name as full_name,
                       t.source as source,
                       t.table_name as table_name,
                       t.row_count as row_count,
                       t.column_count as column_count,
                       t.owner as owner,
                       s.name as source_name,
                       count(sim) as similarity_count
                ORDER BY t.source, t.table_name
            """)
            
            return [dict(record) for record in result]
    
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
        print("✅ Neo4j connection closed")


# ============================================
# MAIN ORCHESTRATION
# ============================================

def build_federated_graph():
    """
    Main function to build the complete federated knowledge graph.
    Combines Snowflake (Olist) and Databricks (Sales/Feedback) data.
    """
    print("\n" + "="*70)
    print("🌐 BUILDING FEDERATED KNOWLEDGE GRAPH")
    print("="*70)
    print("Sources: Snowflake (Olist) + Databricks (Sales/Feedback)")
    print("Privacy: Only fingerprints shared, no raw data")
    print("="*70)
    
    # Initialize builders
    kg_builder = FederatedKGBuilder()
    kg_builder.create_constraints()
    
    # Step 1: Add Snowflake tables (from existing OlistData)
    print("\n" + "📦 STEP 1: Adding Snowflake tables...")
    kg_builder.add_snowflake_tables_as_federated()
    
    # Step 2: Extract and add Databricks tables
    print("\n" + "📦 STEP 2: Extracting Databricks metadata...")
    try:
        db_extractor = DatabricksMetadataExtractor()
        fingerprints = db_extractor.extract_all_fingerprints(
            catalog='workspace',
            schema='sample_data'
        )
        
        kg_builder.add_databricks_tables(fingerprints)
    except Exception as e:
        print(f"⚠️ Databricks extraction failed: {e}")
        print("   Continuing with Snowflake data only...")
    
    # Step 3: Compute cross-source similarities
    print("\n" + "📦 STEP 3: Computing cross-source similarities...")
    kg_builder.compute_cross_source_similarities(threshold=0.3)
    
    # Step 4: Report statistics
    stats = kg_builder.get_federated_statistics()
    
    print("\n" + "="*70)
    print("✨ FEDERATED KNOWLEDGE GRAPH BUILT!")
    print("="*70)
    print(f"📊 Statistics:")
    print(f"   Total Federated Tables: {stats['total_federated_tables']}")
    print(f"   Cross-Source Similarities: {stats['cross_source_similarities']}")
    print(f"   By Source:")
    for source, count in stats.get('by_source', {}).items():
        print(f"      - {source}: {count} tables")
    
    kg_builder.close()
    
    return stats


if __name__ == "__main__":
    build_federated_graph()