import logging
from extractors.metadata_extractor import SnowflakeMetadataExtractor
from knowledge_graph.kg_builder import KnowledgeGraphBuilder
import json
import os
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    # Step 1: Extract metadata from Snowflake
    print("🔍 Extracting metadata from Snowflake...")
    extractor = SnowflakeMetadataExtractor()
    metadata = extractor.extract_all_metadata()
    
    # Save metadata for inspection
    with open('snowflake_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"✅ Extracted metadata for {len(metadata)} databases")
    
    # Step 2: Build Knowledge Graph
    print("🏗️ Building Knowledge Graph...")
    kg_builder = KnowledgeGraphBuilder(
    uri="bolt://localhost:7687",
    user="neo4j",
    password=os.getenv('NEO4J_PASSWORD') )
    
    # Create nodes for each table
    for db in metadata:
        for schema in metadata[db]:
            for table in metadata[db][schema]:
                kg_builder.create_data_asset_node(metadata[db][schema][table])
    
    # Create relationships
    kg_builder.create_relationships(metadata)
    
    print("✅ Knowledge Graph built successfully!")
    
    # Step 3: Run sample queries
    print("🔍 Running duplicate detection...")
    # Your duplicate detection logic here
    
    kg_builder.close()

if __name__ == "__main__":
    main()