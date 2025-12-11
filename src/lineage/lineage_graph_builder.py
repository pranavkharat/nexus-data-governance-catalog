# src/lineage/lineage_graph_builder.py

"""
Lineage Graph Builder
Creates DERIVES_FROM relationships in Neo4j based on extracted lineage.

Supports:
- Snowflake (OlistData nodes)
- Databricks (FederatedTable nodes)

Integrates with existing NEXUS knowledge graph.
"""

from neo4j import GraphDatabase
import os
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

# Import LineageEdge from extractor
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from src.lineage.snowflake_lineage_extractor import LineageEdge
except ImportError:
    # Fallback definition if import fails
    from dataclasses import dataclass
    
    @dataclass
    class LineageEdge:
        source_schema: str
        source_table: str
        target_schema: str
        target_table: str
        lineage_type: str
        confidence: float
        query_id: Optional[str] = None
        query_text: Optional[str] = None
        discovered_at: Optional[datetime] = None

load_dotenv()


class LineageGraphBuilder:
    """
    Builds lineage relationships in Neo4j.
    
    Creates [:DERIVES_FROM] edges between:
    - OlistData nodes (Snowflake)
    - FederatedTable nodes (Databricks)
    
    Enables lineage queries like:
    - "What tables feed into CLIENT_DATA?"
    - "What downstream tables use CUSTOMERS?"
    - "What does customer_feedback derive from?"
    """
    
    def __init__(self):
        """Initialize Neo4j connection."""
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        
        # Verify connection
        with self.driver.session() as session:
            result = session.run("RETURN 1 as test")
            result.single()
        
        print("✅ Connected to Neo4j for lineage building")
        
        # Stats tracking
        self.stats = {
            'edges_created': 0,
            'edges_updated': 0,
            'edges_skipped': 0,
            'errors': 0
        }
    
    def build_lineage_graph(self, lineage_edges: List[LineageEdge]) -> Dict:
        """
        Create DERIVES_FROM relationships in Neo4j for Snowflake tables.
        
        Args:
            lineage_edges: List of LineageEdge objects from extractor
            
        Returns:
            Stats dictionary with creation results
        """
        print("\n" + "="*70)
        print("🔗 BUILDING LINEAGE GRAPH IN NEO4J")
        print("="*70)
        
        for edge in lineage_edges:
            try:
                self._create_derives_from_edge(edge)
            except Exception as e:
                print(f"❌ Error creating edge {edge}: {e}")
                self.stats['errors'] += 1
        
        # Also build Databricks lineage
        self.build_databricks_lineage()
        
        print(f"\n📊 LINEAGE BUILD STATS:")
        print(f"   Created: {self.stats['edges_created']}")
        print(f"   Updated: {self.stats['edges_updated']}")
        print(f"   Skipped: {self.stats['edges_skipped']}")
        print(f"   Errors: {self.stats['errors']}")
        
        return self.stats
    
    def build_databricks_lineage(self):
        """
        Build lineage edges for Databricks tables.
        
        Known lineage:
        - customer_feedback DERIVES_FROM sales_transactions (via transaction_id FK)
        """
        print("\n" + "-"*50)
        print("🧱 BUILDING DATABRICKS LINEAGE")
        print("-"*50)
        
        with self.driver.session() as session:
            # Create lineage: customer_feedback derives from sales_transactions
            result = session.run("""
                MATCH (feedback:FederatedTable {table_name: 'customer_feedback', source: 'databricks'})
                MATCH (sales:FederatedTable {table_name: 'sales_transactions', source: 'databricks'})
                MERGE (feedback)-[r:DERIVES_FROM]->(sales)
                ON CREATE SET 
                    r.lineage_type = 'FOREIGN_KEY',
                    r.confidence = 1.0,
                    r.join_column = 'transaction_id',
                    r.source = 'databricks',
                    r.discovered_at = datetime(),
                    r.created_at = datetime(),
                    r.description = 'Feedback linked to transactions via transaction_id'
                ON MATCH SET
                    r.updated_at = datetime()
                RETURN feedback.table_name AS from_table, 
                       sales.table_name AS to_table,
                       r.lineage_type AS type,
                       CASE WHEN r.created_at = r.updated_at OR r.updated_at IS NULL 
                            THEN 'created' ELSE 'updated' END AS action
            """)
            
            record = result.single()
            if record:
                action = record['action']
                if action == 'created':
                    self.stats['edges_created'] += 1
                    print(f"   ✅ Created: {record['from_table']} --[DERIVES_FROM]--> {record['to_table']}")
                else:
                    self.stats['edges_updated'] += 1
                    print(f"   🔄 Updated: {record['from_table']} --[DERIVES_FROM]--> {record['to_table']}")
            else:
                print("   ⚠️ Databricks tables not found. Run federation first.")
                self.stats['edges_skipped'] += 1
        
        print("   ✅ Databricks lineage complete!")
    
    def _create_derives_from_edge(self, edge: LineageEdge):
        """
        Create a single DERIVES_FROM relationship for Snowflake/Olist tables.
        
        Edge direction: source --[DERIVES_FROM]--> target
        Meaning: target DERIVES_FROM source (target was created from source)
        """
        with self.driver.session() as session:
            # Check if both nodes exist
            check_query = """
            MATCH (source:OlistData {schema: $src_schema, name: $src_table})
            MATCH (target:OlistData {schema: $tgt_schema, name: $tgt_table})
            RETURN source.name as source, target.name as target
            """
            
            result = session.run(check_query,
                src_schema=edge.source_schema,
                src_table=edge.source_table,
                tgt_schema=edge.target_schema,
                tgt_table=edge.target_table
            )
            
            record = result.single()
            
            if not record:
                print(f"   ⚠️ Skipping: Nodes not found for {edge}")
                self.stats['edges_skipped'] += 1
                return
            
            # Create or update DERIVES_FROM relationship
            create_query = """
            MATCH (source:OlistData {schema: $src_schema, name: $src_table})
            MATCH (target:OlistData {schema: $tgt_schema, name: $tgt_table})
            MERGE (target)-[r:DERIVES_FROM]->(source)
            ON CREATE SET 
                r.lineage_type = $lineage_type,
                r.confidence = $confidence,
                r.query_id = $query_id,
                r.source = 'snowflake',
                r.discovered_at = datetime(),
                r.created_at = datetime()
            ON MATCH SET
                r.lineage_type = $lineage_type,
                r.confidence = CASE WHEN $confidence > r.confidence THEN $confidence ELSE r.confidence END,
                r.updated_at = datetime()
            RETURN r, 
                   CASE WHEN r.created_at = r.updated_at THEN 'created' ELSE 'updated' END as action
            """
            
            result = session.run(create_query,
                src_schema=edge.source_schema,
                src_table=edge.source_table,
                tgt_schema=edge.target_schema,
                tgt_table=edge.target_table,
                lineage_type=edge.lineage_type,
                confidence=edge.confidence,
                query_id=edge.query_id
            )
            
            record = result.single()
            
            if record:
                action = record['action']
                if action == 'created':
                    self.stats['edges_created'] += 1
                    print(f"   ✅ Created: {edge.target_table} --[DERIVES_FROM]--> {edge.source_table}")
                else:
                    self.stats['edges_updated'] += 1
                    print(f"   🔄 Updated: {edge.target_table} --[DERIVES_FROM]--> {edge.source_table}")
    
    def get_upstream_lineage(self, schema: str, table: str, depth: int = 3) -> List[Dict]:
        """
        Get all upstream tables (sources) for a given table.
        Supports both OlistData and FederatedTable nodes.
        
        "What tables does CLIENT_DATA derive from?"
        
        Args:
            schema: Table schema (e.g., 'OLIST_MARKETING') or 'databricks' for Databricks tables
            table: Table name (e.g., 'CLIENT_DATA' or 'customer_feedback')
            depth: How many levels upstream to traverse (1-5)
            
        Returns:
            List of upstream table info
        """
        safe_depth = min(max(1, depth), 5)
        
        with self.driver.session() as session:
            # Try OlistData first
            query = f"""
            MATCH path = (target:OlistData {{schema: $schema, name: $table}})
                         -[:DERIVES_FROM*1..{safe_depth}]->(source:OlistData)
            WITH source, 
                 length(path) as distance,
                 [r IN relationships(path) | r.lineage_type][0] as lineage_type,
                 [r IN relationships(path) | r.confidence][0] as confidence
            RETURN DISTINCT 
                source.schema as schema,
                source.name as table,
                source.row_count as rows,
                'snowflake' as source_type,
                distance,
                lineage_type,
                confidence
            ORDER BY distance ASC
            """
            
            result = session.run(query, schema=schema, table=table)
            olist_results = [dict(record) for record in result]
            
            # Also try FederatedTable
            query_federated = f"""
            MATCH path = (target:FederatedTable {{table_name: $table}})
                         -[:DERIVES_FROM*1..{safe_depth}]->(source:FederatedTable)
            WITH source, 
                 length(path) as distance,
                 [r IN relationships(path) | r.lineage_type][0] as lineage_type,
                 [r IN relationships(path) | r.confidence][0] as confidence
            RETURN DISTINCT 
                source.source as schema,
                source.table_name as table,
                source.row_count as rows,
                'databricks' as source_type,
                distance,
                lineage_type,
                confidence
            ORDER BY distance ASC
            """
            
            result = session.run(query_federated, table=table)
            federated_results = [dict(record) for record in result]
            
            return olist_results + federated_results
    
    def get_downstream_lineage(self, schema: str, table: str, depth: int = 3) -> List[Dict]:
        """
        Get all downstream tables (targets) that derive from this table.
        Supports both OlistData and FederatedTable nodes.
        
        "What tables were created from CUSTOMERS?"
        
        Args:
            schema: Table schema (e.g., 'OLIST_SALES') or 'databricks' for Databricks tables
            table: Table name (e.g., 'CUSTOMERS' or 'sales_transactions')
            depth: How many levels downstream to traverse (1-5)
            
        Returns:
            List of downstream table info
        """
        safe_depth = min(max(1, depth), 5)
        
        with self.driver.session() as session:
            # Try OlistData first
            query = f"""
            MATCH path = (source:OlistData {{schema: $schema, name: $table}})
                         <-[:DERIVES_FROM*1..{safe_depth}]-(target:OlistData)
            WITH target, 
                 length(path) as distance,
                 [r IN relationships(path) | r.lineage_type][0] as lineage_type,
                 [r IN relationships(path) | r.confidence][0] as confidence
            RETURN DISTINCT 
                target.schema as schema,
                target.name as table,
                target.row_count as rows,
                'snowflake' as source_type,
                distance,
                lineage_type,
                confidence
            ORDER BY distance ASC
            """
            
            result = session.run(query, schema=schema, table=table)
            olist_results = [dict(record) for record in result]
            
            # Also try FederatedTable
            query_federated = f"""
            MATCH path = (source:FederatedTable {{table_name: $table}})
                         <-[:DERIVES_FROM*1..{safe_depth}]-(target:FederatedTable)
            WITH target, 
                 length(path) as distance,
                 [r IN relationships(path) | r.lineage_type][0] as lineage_type,
                 [r IN relationships(path) | r.confidence][0] as confidence
            RETURN DISTINCT 
                target.source as schema,
                target.table_name as table,
                target.row_count as rows,
                'databricks' as source_type,
                distance,
                lineage_type,
                confidence
            ORDER BY distance ASC
            """
            
            result = session.run(query_federated, table=table)
            federated_results = [dict(record) for record in result]
            
            return olist_results + federated_results
    
    def get_full_lineage_graph(self) -> Dict:
        """
        Get the complete lineage graph for visualization.
        Includes both Snowflake and Databricks lineage.
        
        Returns:
            Dict with nodes and edges for graph visualization
        """
        with self.driver.session() as session:
            nodes = set()
            edges = []
            
            # Get OlistData lineage relationships
            query_olist = """
            MATCH (target:OlistData)-[r:DERIVES_FROM]->(source:OlistData)
            RETURN 
                source.schema + '.' + source.name as source,
                target.schema + '.' + target.name as target,
                r.lineage_type as type,
                r.confidence as confidence,
                'snowflake' as platform
            """
            
            result = session.run(query_olist)
            for record in result:
                nodes.add(record['source'])
                nodes.add(record['target'])
                edges.append({
                    'source': record['source'],
                    'target': record['target'],
                    'type': record['type'],
                    'confidence': record['confidence'],
                    'platform': record['platform']
                })
            
            # Get FederatedTable lineage relationships (Databricks)
            query_federated = """
            MATCH (target:FederatedTable)-[r:DERIVES_FROM]->(source:FederatedTable)
            RETURN 
                'databricks.' + source.table_name as source,
                'databricks.' + target.table_name as target,
                r.lineage_type as type,
                r.confidence as confidence,
                'databricks' as platform
            """
            
            result = session.run(query_federated)
            for record in result:
                nodes.add(record['source'])
                nodes.add(record['target'])
                edges.append({
                    'source': record['source'],
                    'target': record['target'],
                    'type': record['type'],
                    'confidence': record['confidence'],
                    'platform': record['platform']
                })
            
            return {
                'nodes': list(nodes),
                'edges': edges,
                'node_count': len(nodes),
                'edge_count': len(edges)
            }
    
    def get_lineage_statistics(self) -> Dict:
        """Get statistics about lineage in the graph (both sources)."""
        with self.driver.session() as session:
            # Combined stats from all DERIVES_FROM relationships
            query = """
            MATCH ()-[r:DERIVES_FROM]->()
            WITH r
            RETURN 
                count(r) as total_edges,
                avg(r.confidence) as avg_confidence,
                collect(DISTINCT r.lineage_type) as lineage_types,
                collect(DISTINCT coalesce(r.source, 'unknown')) as sources
            """
            
            result = session.run(query)
            record = result.single()
            
            if record:
                return {
                    'total_lineage_edges': record['total_edges'],
                    'avg_confidence': record['avg_confidence'] or 0,
                    'lineage_types': record['lineage_types'],
                    'sources': record['sources']
                }
            
            return {'total_lineage_edges': 0}
    
    def get_databricks_lineage_summary(self) -> Dict:
        """Get Databricks-specific lineage summary."""
        with self.driver.session() as session:
            query = """
            MATCH (target:FederatedTable)-[r:DERIVES_FROM]->(source:FederatedTable)
            RETURN 
                source.table_name as source_table,
                target.table_name as target_table,
                r.lineage_type as type,
                r.confidence as confidence,
                r.join_column as join_column
            """
            
            result = session.run(query)
            edges = [dict(record) for record in result]
            
            return {
                'databricks_lineage_edges': len(edges),
                'edges': edges
            }
    
    def delete_all_lineage(self):
        """Remove all DERIVES_FROM relationships (for testing)."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH ()-[r:DERIVES_FROM]->()
                DELETE r
                RETURN count(r) as deleted
            """)
            deleted = result.single()['deleted']
            print(f"🗑️ Deleted {deleted} DERIVES_FROM relationships")
    
    def close(self):
        """Close Neo4j connection."""
        self.driver.close()
        print("✅ Neo4j connection closed")


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🧪 TESTING LINEAGE GRAPH BUILDER")
    print("="*70)
    
    builder = LineageGraphBuilder()
    
    # Create test lineage edges (Olist ground truth)
    test_edges = [
        LineageEdge(
            source_schema='OLIST_SALES',
            source_table='CUSTOMERS',
            target_schema='OLIST_MARKETING',
            target_table='CLIENT_DATA',
            lineage_type='CTAS',
            confidence=1.0
        ),
        LineageEdge(
            source_schema='OLIST_SALES',
            source_table='CUSTOMERS',
            target_schema='OLIST_ANALYTICS',
            target_table='CUSTOMER_MASTER',
            lineage_type='TRANSFORM',
            confidence=0.85
        ),
        LineageEdge(
            source_schema='OLIST_SALES',
            source_table='ORDERS',
            target_schema='OLIST_MARKETING',
            target_table='SALES_ORDERS',
            lineage_type='CTAS',
            confidence=1.0
        ),
        LineageEdge(
            source_schema='OLIST_SALES',
            source_table='ORDERS',
            target_schema='OLIST_ANALYTICS',
            target_table='PURCHASE_HISTORY',
            lineage_type='TRANSFORM',
            confidence=0.62
        ),
        LineageEdge(
            source_schema='OLIST_SALES',
            source_table='PRODUCTS',
            target_schema='OLIST_MARKETING',
            target_table='PRODUCT_CATALOG',
            lineage_type='CTAS',
            confidence=1.0
        ),
    ]
    
    # Build lineage graph (includes Databricks automatically)
    stats = builder.build_lineage_graph(test_edges)
    
    # Test upstream query for Snowflake
    print("\n" + "="*70)
    print("🔼 UPSTREAM LINEAGE: What does CLIENT_DATA derive from?")
    print("-"*70)
    
    upstream = builder.get_upstream_lineage('OLIST_MARKETING', 'CLIENT_DATA')
    for table in upstream:
        print(f"   {table['schema']}.{table['table']} ({table['source_type']}, distance: {table['distance']})")
    
    # Test downstream query for Snowflake
    print("\n" + "="*70)
    print("🔽 DOWNSTREAM LINEAGE: What tables derive from CUSTOMERS?")
    print("-"*70)
    
    downstream = builder.get_downstream_lineage('OLIST_SALES', 'CUSTOMERS')
    for table in downstream:
        print(f"   {table['schema']}.{table['table']} ({table['source_type']}, distance: {table['distance']})")
    
    # Test Databricks lineage
    print("\n" + "="*70)
    print("🧱 DATABRICKS LINEAGE: What does customer_feedback derive from?")
    print("-"*70)
    
    upstream_db = builder.get_upstream_lineage('databricks', 'customer_feedback')
    if upstream_db:
        for table in upstream_db:
            print(f"   {table['schema']}.{table['table']} ({table['source_type']}, type: {table['lineage_type']})")
    else:
        print("   (No Databricks lineage found - run federation first)")
    
    # Test Databricks downstream
    print("\n" + "="*70)
    print("🧱 DATABRICKS LINEAGE: What derives from sales_transactions?")
    print("-"*70)
    
    downstream_db = builder.get_downstream_lineage('databricks', 'sales_transactions')
    if downstream_db:
        for table in downstream_db:
            print(f"   {table['schema']}.{table['table']} ({table['source_type']}, type: {table['lineage_type']})")
    else:
        print("   (No Databricks lineage found - run federation first)")
    
    # Get full graph
    print("\n" + "="*70)
    print("📊 FULL LINEAGE GRAPH (ALL SOURCES)")
    print("-"*70)
    
    graph = builder.get_full_lineage_graph()
    print(f"   Total Nodes: {graph['node_count']}")
    print(f"   Total Edges: {graph['edge_count']}")
    
    print("\n   Snowflake edges:")
    for edge in graph['edges']:
        if edge['platform'] == 'snowflake':
            print(f"      {edge['target']} --[{edge['type']}]--> {edge['source']}")
    
    print("\n   Databricks edges:")
    for edge in graph['edges']:
        if edge['platform'] == 'databricks':
            print(f"      {edge['target']} --[{edge['type']}]--> {edge['source']}")
    
    # Get Databricks-specific summary
    print("\n" + "="*70)
    print("🧱 DATABRICKS LINEAGE SUMMARY")
    print("-"*70)
    
    db_summary = builder.get_databricks_lineage_summary()
    print(f"   Databricks lineage edges: {db_summary['databricks_lineage_edges']}")
    for edge in db_summary['edges']:
        print(f"   {edge['target_table']} --[{edge['type']}]--> {edge['source_table']} (via {edge['join_column']})")
    
    # Statistics
    print("\n" + "="*70)
    print("📈 COMBINED LINEAGE STATISTICS")
    print("-"*70)
    
    stats = builder.get_lineage_statistics()
    print(f"   Total edges: {stats.get('total_lineage_edges', 0)}")
    print(f"   Avg confidence: {stats.get('avg_confidence', 0):.2%}")
    print(f"   Types: {stats.get('lineage_types', [])}")
    print(f"   Sources: {stats.get('sources', [])}")
    
    builder.close()