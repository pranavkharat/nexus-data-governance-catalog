from knowledge_graph.kg_builder import KnowledgeGraphBuilder
import json

def load_metadata_to_kg():
    print("🏗️ Loading JSON metadata to Knowledge Graph")
    print("=" * 50)
    
    # Load your JSON file
    with open('snowflake_metadata.json', 'r') as f:
        metadata = json.load(f)
    
    # Count tables
    total_tables = sum(len(metadata[db][schema]) for db in metadata for schema in metadata[db])
    print(f"Found {total_tables} tables in metadata")
    
    # Connect to Neo4j
    print("\n⚠️ Make sure Neo4j is running!")
    neo4j_password = input("Enter Neo4j password (press Enter for 'password'): ").strip() or "password"
    
    kg_builder = KnowledgeGraphBuilder(
        uri="bolt://localhost:7687",
        user="neo4j",
        password=neo4j_password
    )
    
    # Clear existing data
    print("\nClearing existing graph...")
    kg_builder.clear_graph()
    
    # Create nodes
    print("Creating nodes...")
    for db in metadata:
        for schema in metadata[db]:
            for table_name in metadata[db][schema]:
                table_data = metadata[db][schema][table_name]
                kg_builder.create_data_asset_node(table_data)
    
    # Create relationships
    print("Creating relationships...")
    kg_builder.create_relationships(metadata)
    
    # Get statistics
    stats = kg_builder.get_statistics()
    print(f"\n📊 Graph Statistics:")
    print(f"  - Nodes: {stats['nodes']}")
    print(f"  - Duplicate relationships: {stats['duplicates']}")
    print(f"  - Lineage relationships: {stats['lineage']}")
    
    kg_builder.close()
    
    print("\n✅ Knowledge Graph loaded successfully!")
    print("\n📌 View in Neo4j Browser:")
    print("1. Open http://localhost:7474")
    print("2. Run: MATCH (n) RETURN n LIMIT 50")

if __name__ == "__main__":
    load_metadata_to_kg()