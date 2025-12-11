import os
from dotenv import load_dotenv
from connectors.snowflake_connector import SnowflakeConnector

load_dotenv()

print("🚀 Testing Final Snowflake Connection")
print("=" * 50)

connector = SnowflakeConnector()

try:
    # Connect
    connector.connect()
    print("✅ Connected successfully!")
    
    # Get databases
    print("\n📂 Getting databases...")
    databases = connector.get_databases()
    print(f"Found {len(databases)} databases")
    for db in databases[:5]:
        print(f"  - {db['name']}")
    
    # Get schemas in TRAINING_DB
    print("\n📁 Getting schemas in TRAINING_DB...")
    schemas = connector.get_schemas('TRAINING_DB')
    print(f"Found {len(schemas)} schemas")
    for schema in schemas[:5]:
        print(f"  - {schema['name']}")
    
    # Get tables in WEATHER schema
    print("\n📊 Getting tables in WEATHER schema...")
    tables = connector.get_tables('TRAINING_DB', 'WEATHER')
    print(f"Found {len(tables)} tables")
    for table in tables[:5]:
        print(f"  - {table['name']}")
    
    # Get metadata for first table
    if tables:
        first_table = tables[0]['name']
        print(f"\n🔍 Getting metadata for {first_table}...")
        metadata = connector.get_table_metadata('TRAINING_DB', 'WEATHER', first_table)
        print(f"  Columns: {len(metadata['columns'])}")
        print(f"  Row count: {metadata['row_count']}")
    
    connector.close()
    print("\n✅ All tests passed! Ready to build Knowledge Graph!")
    
except Exception as e:
    print(f"❌ Error: {e}")