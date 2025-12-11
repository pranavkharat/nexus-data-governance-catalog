# knowledge_graph/olist_kg_builder.py

"""
Separate KG Builder for Olist data
Keeps Olist graph separate from TPC graph in Neo4j
"""

from neo4j import GraphDatabase
from collections import defaultdict


class OlistKGBuilder:
    """Build Knowledge Graph specifically for Olist E-Commerce data"""
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def build_olist_graph(self, metadata):
        """
        Build KG only for Olist schemas (OLIST_*)
        Keeps TPC data untouched
        """
        
        print("🏗️  Building Olist Knowledge Graph")
        print("=" * 60)
        
        # Filter to only Olist schemas
        olist_metadata = self._filter_olist_data(metadata)
        
        if not olist_metadata:
            print("❌ No Olist data found in metadata")
            return
        
        # Create nodes
        node_count = self._create_olist_nodes(olist_metadata)
        
        # Detect duplicates
        dup_count = self._detect_olist_duplicates(olist_metadata)
        
        # Create business relationships
        rel_count = self._create_olist_relationships()
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 OLIST KNOWLEDGE GRAPH BUILT")
        print("=" * 60)
        print(f"  Nodes created: {node_count}")
        print(f"  Duplicate relationships: {dup_count}")
        print(f"  Business relationships: {rel_count}")
        print("=" * 60)
    
    def _filter_olist_data(self, metadata):
        """Extract only Olist-related tables"""
        olist_data = {}
        
        for db in metadata:
            for schema in metadata[db]:
                # Only process schemas starting with OLIST_
                if schema.startswith('OLIST_'):
                    if db not in olist_data:
                        olist_data[db] = {}
                    olist_data[db][schema] = metadata[db][schema]
        
        return olist_data
    
    def _create_olist_nodes(self, olist_metadata):
        """Create DataAsset nodes for Olist tables"""
        
        count = 0
        
        with self.driver.session() as session:
            for db in olist_metadata:
                for schema in olist_metadata[db]:
                    for table_name, table_data in olist_metadata[db][schema].items():
                        
                        query = """
                        MERGE (d:OlistData {
                            full_name: $full_name
                        })
                        SET d.name = $name,
                            d.database = $database,
                            d.schema = $schema,
                            d.row_count = $row_count,
                            d.fingerprint = $fingerprint,
                            d.column_count = $column_count,
                            d.data_source = 'Olist E-Commerce'
                        """
                        
                        session.run(query,
                            full_name=table_data['full_name'],
                            name=table_data['table'],
                            database=table_data['database'],
                            schema=table_data['schema'],
                            row_count=table_data['row_count'],
                            fingerprint=table_data['fingerprint']['column_signature'],
                            column_count=table_data['fingerprint']['column_count']
                        )
                        
                        count += 1
                        print(f"  ✓ Created: {table_data['full_name']}")
        
        return count
    
    def _detect_olist_duplicates(self, olist_metadata):
        """Detect duplicates within Olist data"""
        
        # Collect all Olist tables
        all_tables = []
        for db in olist_metadata:
            for schema in olist_metadata[db]:
                for table_name, table_data in olist_metadata[db][schema].items():
                    all_tables.append(table_data)
        
        print(f"\n🔍 Analyzing {len(all_tables)} Olist tables for duplicates...")
        
        # Group by fingerprint (exact duplicates)
        fingerprint_groups = defaultdict(list)
        for table in all_tables:
            fp = table['fingerprint']['column_signature']
            fingerprint_groups[fp].append(table)
        
        duplicate_count = 0
        
        with self.driver.session() as session:
            # Exact duplicates
            for fp, tables in fingerprint_groups.items():
                if len(tables) > 1:
                    print(f"\n  📌 Exact duplicate group:")
                    for t in tables:
                        print(f"     - {t['full_name']}")
                    
                    # Create relationships between all pairs
                    for i in range(len(tables)):
                        for j in range(i+1, len(tables)):
                            query = """
                            MATCH (t1:OlistData {full_name: $table1})
                            MATCH (t2:OlistData {full_name: $table2})
                            MERGE (t1)-[r:OLIST_DUPLICATE {
                                match_type: 'EXACT_SCHEMA',
                                confidence: 1.0,
                                basis: 'identical_fingerprint'
                            }]->(t2)
                            """
                            session.run(query, 
                                      table1=tables[i]['full_name'],
                                      table2=tables[j]['full_name'])
                            duplicate_count += 1
            
            # Partial duplicates (column overlap)
            for i, table1 in enumerate(all_tables):
                cols1 = set([c['column_name'] for c in table1['columns']])
                
                for table2 in all_tables[i+1:]:
                    cols2 = set([c['column_name'] for c in table2['columns']])
                    
                    if len(cols1) == 0 or len(cols2) == 0:
                        continue
                    
                    overlap = len(cols1 & cols2) / min(len(cols1), len(cols2))
                    
                    # If 50-99% overlap, it's a partial duplicate
                    if 0.5 <= overlap < 1.0:
                        print(f"\n  🔸 Partial match ({overlap*100:.0f}%):")
                        print(f"     {table1['full_name']}")
                        print(f"     {table2['full_name']}")
                        
                        query = """
                        MATCH (t1:OlistData {full_name: $table1})
                        MATCH (t2:OlistData {full_name: $table2})
                        MERGE (t1)-[r:OLIST_DUPLICATE {
                            match_type: 'PARTIAL_COLUMNS',
                            confidence: $confidence,
                            overlap_pct: $overlap,
                            basis: 'column_overlap'
                        }]->(t2)
                        """
                        session.run(query, 
                                  table1=table1['full_name'],
                                  table2=table2['full_name'],
                                  confidence=overlap,
                                  overlap=overlap)
                        duplicate_count += 1
        
        return duplicate_count
    
    def _create_olist_relationships(self):
        """Create business relationships for Olist data model"""
        
        print("\n🔗 Creating Olist business relationships...")
        
        rel_count = 0
        
        with self.driver.session() as session:
            
            # ORDERS → CUSTOMERS
            query = """
            MATCH (o:OlistData), (c:OlistData)
            WHERE o.name = 'ORDERS' AND c.name = 'CUSTOMERS'
              AND o.schema = c.schema
              AND o.schema STARTS WITH 'OLIST_'
            MERGE (o)-[:BELONGS_TO {type: 'customer_order'}]->(c)
            """
            result = session.run(query)
            rel_count += result.consume().counters.relationships_created
            print("  ✓ ORDERS → CUSTOMERS")
            
            # ORDER_ITEMS → ORDERS
            query = """
            MATCH (oi:OlistData), (o:OlistData)
            WHERE oi.name = 'ORDER_ITEMS' AND o.name = 'ORDERS'
              AND oi.schema = o.schema
              AND oi.schema STARTS WITH 'OLIST_'
            MERGE (oi)-[:DERIVED_FROM {type: 'order_detail'}]->(o)
            """
            result = session.run(query)
            rel_count += result.consume().counters.relationships_created
            print("  ✓ ORDER_ITEMS → ORDERS")
            
            # ORDER_PAYMENTS → ORDERS
            query = """
            MATCH (op:OlistData), (o:OlistData)
            WHERE op.name = 'ORDER_PAYMENTS' AND o.name = 'ORDERS'
              AND op.schema = o.schema
              AND op.schema STARTS WITH 'OLIST_'
            MERGE (op)-[:PAYMENT_FOR]->(o)
            """
            result = session.run(query)
            rel_count += result.consume().counters.relationships_created
            print("  ✓ ORDER_PAYMENTS → ORDERS")
            
            # ORDER_REVIEWS → ORDERS
            query = """
            MATCH (or:OlistData), (o:OlistData)
            WHERE or.name = 'ORDER_REVIEWS' AND o.name = 'ORDERS'
              AND or.schema = o.schema
              AND or.schema STARTS WITH 'OLIST_'
            MERGE (or)-[:REVIEWS]->(o)
            """
            result = session.run(query)
            rel_count += result.consume().counters.relationships_created
            print("  ✓ ORDER_REVIEWS → ORDERS")
            
            # ORDER_ITEMS → PRODUCTS
            query = """
            MATCH (oi:OlistData), (p:OlistData)
            WHERE oi.name = 'ORDER_ITEMS' AND p.name = 'PRODUCTS'
              AND oi.schema = p.schema
              AND oi.schema STARTS WITH 'OLIST_'
            MERGE (oi)-[:CONTAINS]->(p)
            """
            result = session.run(query)
            rel_count += result.consume().counters.relationships_created
            print("  ✓ ORDER_ITEMS → PRODUCTS")
            
            # ORDER_ITEMS → SELLERS
            query = """
            MATCH (oi:OlistData), (s:OlistData)
            WHERE oi.name = 'ORDER_ITEMS' AND s.name = 'SELLERS'
              AND oi.schema = s.schema
              AND oi.schema STARTS WITH 'OLIST_'
            MERGE (oi)-[:SOLD_BY]->(s)
            """
            result = session.run(query)
            rel_count += result.consume().counters.relationships_created
            print("  ✓ ORDER_ITEMS → SELLERS")
        
        return rel_count
    
    def close(self):
        self.driver.close()