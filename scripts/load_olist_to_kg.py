# load_olist_to_kg.py

"""
Extract Olist metadata and load to Neo4j Knowledge Graph
Completely separate from TPC data pipeline
"""

from src.extractors.metadata_extractor import SnowflakeMetadataExtractor
from src.knowledge_graph.olist_kg_builder import OlistKGBuilder
import json
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_olist_metadata():
    """Extract metadata only for Olist schemas"""
    
    print("\n🔍 Extracting Olist metadata from Snowflake...")
    print("=" * 60)
    
    extractor = SnowflakeMetadataExtractor()
    extractor.connector.connect()
    
    olist_metadata = {'TRAINING_DB': {}}
    
    try:
        cursor = extractor.connector.connection.cursor()
        cursor.execute("USE DATABASE TRAINING_DB")
        
        # Get all schemas
        cursor.execute("SHOW SCHEMAS IN DATABASE TRAINING_DB")
        schemas = cursor.fetchall()
        
        for schema in schemas:
            schema_name = schema[1]
            
            # Filter to only OLIST_ schemas
            if not schema_name.startswith('OLIST_'):
                continue
            
            print(f"\n📁 Processing schema: {schema_name}")
            olist_metadata['TRAINING_DB'][schema_name] = {}
            
            # Get tables
            cursor.execute(f"SHOW TABLES IN TRAINING_DB.{schema_name}")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[1]
                
                print(f"  📊 Extracting: {table_name}...", end="")
                
                # Extract metadata using your existing extractor
                table_metadata = extractor.extract_table_metadata(
                    'TRAINING_DB', schema_name, table_name
                )
                
                if table_metadata:
                    olist_metadata['TRAINING_DB'][schema_name][table_name] = table_metadata
                    print(" ✓")
                else:
                    print(" ✗")
        
        cursor.close()
    
    finally:
        extractor.connector.close()
    
    # Save Olist metadata separately
    with open('olist_metadata.json', 'w') as f:
        json.dump(olist_metadata, f, indent=2, default=str)
    
    print(f"\n✅ Saved Olist metadata to: olist_metadata.json")
    
    return olist_metadata


def load_olist_to_neo4j(metadata):
    """Load Olist data to Neo4j (doesn't touch TPC data)"""
    
    print("\n🗄️  Loading Olist data to Neo4j")
    print("=" * 60)
    
    # Use Neo4j password from environment variable
    neo4j_password = os.getenv('NEO4J_PASSWORD')
    
    if not neo4j_password:
        raise ValueError("NEO4J_PASSWORD not found in .env file!")
    
    kg_builder = OlistKGBuilder(
        uri="bolt://localhost:7687",
        user="neo4j",
        password=neo4j_password
    )
    
    # Build Olist graph
    kg_builder.build_olist_graph(metadata)
    
    kg_builder.close()


def main():
    """Complete Olist pipeline"""
    
    input("Press Enter to continue...")
    
    try:
        # Step 1: Extract
        olist_metadata = extract_olist_metadata()
        
        # Step 2: Load to Neo4j
        load_olist_to_neo4j(olist_metadata)
        
        logger.info("✅ Olist Knowledge Graph pipeline completed successfully!") 
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()