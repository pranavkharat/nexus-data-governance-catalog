from connectors.snowflake_connector import SnowflakeConnector
import json

print("🔍 Exploring Accessible Databases...")

connector = SnowflakeConnector()
connector.connect()

# Get all databases
cursor = connector.connection.cursor()
cursor.execute("SHOW DATABASES")
all_databases = cursor.fetchall()

print(f"\n📂 All databases (found {len(all_databases)}):")
accessible_dbs = []

for db in all_databases:
    db_name = db[1]  # Database name is usually in position 1
    print(f"  - {db_name}", end="")
    
    # Try to access it
    try:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name}")
        schemas = cursor.fetchall()
        print(f" ✅ (Accessible - {len(schemas)} schemas)")
        accessible_dbs.append(db_name)
    except Exception as e:
        print(f" ❌ (No access)")

print(f"\n✅ Accessible databases: {accessible_dbs}")

# Focus on TRAINING_DB
print("\n📁 Exploring TRAINING_DB schemas...")
cursor.execute("SHOW SCHEMAS IN DATABASE TRAINING_DB")
schemas = cursor.fetchall()

metadata = {}
for schema in schemas:
    schema_name = schema[1]
    if schema_name in ['INFORMATION_SCHEMA', 'PUBLIC']:
        continue
    
    print(f"\n  Schema: {schema_name}")
    
    # Get tables
    try:
        cursor.execute(f"SHOW TABLES IN TRAINING_DB.{schema_name}")
        tables = cursor.fetchall()
        
        print(f"    Tables: {len(tables)}")
        for table in tables[:5]:  # Show first 5
            table_name = table[1]
            
            # Get row count
            try:
                cursor.execute(f"SELECT COUNT(*) FROM TRAINING_DB.{schema_name}.{table_name}")
                count = cursor.fetchone()[0]
                print(f"      - {table_name}: {count:,} rows")
            except:
                print(f"      - {table_name}")
    except Exception as e:
        print(f"    Error accessing tables: {e}")

cursor.close()
connector.close()

print("\n✅ Exploration complete!")