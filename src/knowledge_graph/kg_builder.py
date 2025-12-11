from neo4j import GraphDatabase
import json

class KnowledgeGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def create_data_asset_node(self, table_metadata):
        """Create a node in the KG for a data asset"""
        
        with self.driver.session() as session:
            query = """
            CREATE (d:DataAsset {
                name: $table_name,
                full_name: $full_name,
                database: $database,
                schema: $schema,
                row_count: $row_count,
                fingerprint: $fingerprint,
                column_count: $column_count
            })
            """
            
            session.run(query,
                table_name=table_metadata['table'],
                full_name=table_metadata['full_name'],
                database=table_metadata['database'],
                schema=table_metadata['schema'],
                row_count=table_metadata['row_count'],
                fingerprint=table_metadata['fingerprint']['column_signature'],
                column_count=table_metadata['fingerprint']['column_count']
            )
    
    def create_relationships(self, metadata_catalog):
        """Detect and create relationships between data assets"""
        
        # Find potential duplicates based on fingerprints
        self.detect_duplicates(metadata_catalog)
        
        # Create lineage relationships
        self.create_lineage_relationships(metadata_catalog)
        
        # Create ownership relationships
        self.create_ownership_relationships(metadata_catalog)
    
    def detect_duplicates(self, metadata_catalog):
        """Detect potential duplicate tables"""
        
        fingerprints = {}
        
        # Group tables by fingerprint
        for db in metadata_catalog:
            for schema in metadata_catalog[db]:
                for table in metadata_catalog[db][schema]:
                    fp = metadata_catalog[db][schema][table]['fingerprint']['column_signature']
                    
                    if fp not in fingerprints:
                        fingerprints[fp] = []
                    
                    fingerprints[fp].append(metadata_catalog[db][schema][table]['full_name'])
        
        # Create duplicate relationships
        with self.driver.session() as session:
            for fp, tables in fingerprints.items():
                if len(tables) > 1:
                    print(f"Potential duplicates found: {tables}")
                    
                    # Create relationships between duplicates
                    for i in range(len(tables)):
                        for j in range(i+1, len(tables)):
                            query = """
                            MATCH (a:DataAsset {full_name: $table1})
                            MATCH (b:DataAsset {full_name: $table2})
                            CREATE (a)-[:POTENTIAL_DUPLICATE {
                                confidence: 0.85,
                                basis: 'schema_match'
                            }]->(b)
                            """
                            session.run(query, table1=tables[i], table2=tables[j])
    
    def create_lineage_relationships(self, metadata_catalog):
        """Create data lineage relationships based on common patterns"""
        with self.driver.session() as session:
            # LINEITEM relates to ORDERS (detail-master relationship)
            query1 = """
            MATCH (li:DataAsset), (o:DataAsset)
            WHERE li.name = 'LINEITEM' AND o.name = 'ORDERS'
              AND li.schema = o.schema
            MERGE (li)-[:DERIVED_FROM {type: 'detail'}]->(o)
            """
            result = session.run(query1)
            
            # PARTSUPP relates to PART
            query2 = """
            MATCH (ps:DataAsset), (p:DataAsset)
            WHERE ps.name = 'PARTSUPP' AND p.name = 'PART'
              AND ps.schema = p.schema
            MERGE (ps)-[:DERIVED_FROM {type: 'junction'}]->(p)
            """
            result = session.run(query2)
            
            # PARTSUPP relates to SUPPLIER
            query3 = """
            MATCH (ps:DataAsset), (s:DataAsset)
            WHERE ps.name = 'PARTSUPP' AND s.name = 'SUPPLIER'
              AND ps.schema = s.schema
            MERGE (ps)-[:DERIVED_FROM {type: 'junction'}]->(s)
            """
            result = session.run(query3)
            
            # CUSTOMER relates to NATION
            query4 = """
            MATCH (c:DataAsset), (n:DataAsset)
            WHERE c.name = 'CUSTOMER' AND n.name = 'NATION'
              AND c.schema = n.schema
            MERGE (c)-[:REFERENCES]->(n)
            """
            result = session.run(query4)
            
            # SUPPLIER relates to NATION
            query5 = """
            MATCH (s:DataAsset), (n:DataAsset)
            WHERE s.name = 'SUPPLIER' AND n.name = 'NATION'
              AND s.schema = n.schema
            MERGE (s)-[:REFERENCES]->(n)
            """
            result = session.run(query5)
            
            # NATION relates to REGION
            query6 = """
            MATCH (n:DataAsset), (r:DataAsset)
            WHERE n.name = 'NATION' AND r.name = 'REGION'
              AND n.schema = r.schema
            MERGE (n)-[:BELONGS_TO]->(r)
            """
            result = session.run(query6)
    
    def create_ownership_relationships(self, metadata_catalog):
        """Create ownership relationships (placeholder for now)"""
        # This is where you'd add ownership logic if you had that data
        # For now, it's a placeholder since we don't have ownership information
        pass
    
    def clear_graph(self):
        """Clear all existing nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Cleared existing graph")
    
    def get_statistics(self):
        """Get statistics about the graph"""
        with self.driver.session() as session:
            # Count nodes
            node_result = session.run("MATCH (n:DataAsset) RETURN count(n) as count")
            node_count = node_result.single()['count']
            
            # Count duplicate relationships
            dup_result = session.run("MATCH ()-[r:POTENTIAL_DUPLICATE]->() RETURN count(r) as count")
            dup_count = dup_result.single()['count']
            
            # Count lineage relationships
            lineage_result = session.run("MATCH ()-[r:DERIVED_FROM|REFERENCES|BELONGS_TO]->() RETURN count(r) as count")
            lineage_count = lineage_result.single()['count']
            
            return {
                'nodes': node_count,
                'duplicates': dup_count,
                'lineage': lineage_count
            }
    
    def close(self):
        """Close the Neo4j driver connection"""
        self.driver.close()