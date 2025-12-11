import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("🔍 Testing Snowflake Connection...")
print("-" * 50)

# Show what we're using
print("Configuration:")
print(f"  Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print(f"  User: {os.getenv('SNOWFLAKE_USER')}")
print(f"  Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
print(f"  Role: {os.getenv('SNOWFLAKE_ROLE')}")
print(f"  Database: {os.getenv('SNOWFLAKE_DATABASE')}")
print(f"  Schema: {os.getenv('SNOWFLAKE_SCHEMA')}")
print("-" * 50)

try:
    # Connect using your credentials
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        authenticator='externalbrowser'
    )
    
    print("✅ Connected successfully!")
    print("-" * 50)
    
    cursor = conn.cursor()
    
    # Verify connection details
    cursor.execute("""
        SELECT 
            CURRENT_USER() as user,
            CURRENT_ROLE() as role,
            CURRENT_WAREHOUSE() as warehouse,
            CURRENT_DATABASE() as database,
            CURRENT_SCHEMA() as schema
    """)
    result = cursor.fetchone()
    
    print("Current Session Info:")
    print(f"  User: {result[0]}")
    print(f"  Role: {result[1]}")
    print(f"  Warehouse: {result[2]}")
    print(f"  Database: {result[3]}")
    print(f"  Schema: {result[4]}")
    print("-" * 50)
    
    # Explore WEATHER schema
    print("\n📊 Tables in WEATHER schema:")
    cursor.execute("SHOW TABLES IN TRAINING_DB.WEATHER")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[1]
        print(f"  - {table_name}")
        
        # Get row count for each table
        cursor.execute(f"SELECT COUNT(*) FROM TRAINING_DB.WEATHER.{table_name}")
        count = cursor.fetchone()[0]
        print(f"    Rows: {count}")
    
    # Check other schemas
    print("\n📁 Other schemas in TRAINING_DB:")
    cursor.execute("SHOW SCHEMAS IN DATABASE TRAINING_DB")
    for schema in cursor.fetchall():
        schema_name = schema[1]
        if schema_name not in ['WEATHER', 'INFORMATION_SCHEMA', 'PUBLIC']:
            print(f"  - {schema_name}")
    
    cursor.close()
    conn.close()
    print("\n✅ Test completed successfully!")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("\nThis will open your browser for authentication.")
    print("Please login to Snowflake when the browser opens.")