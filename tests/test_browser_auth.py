import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

print("🌐 Testing Browser Authentication")
print("=" * 50)
print("This will open your browser")
print("Please authenticate with your passkey when prompted")
print("=" * 50)

try:
    conn = snowflake.connector.connect(
        account='SFEDU02-UUB90967',
        user='CHIPMUNK',
        authenticator='externalbrowser',
        warehouse='ANIMAL_TASK_WH',
        role='TRAINING_ROLE',
        database='TRAINING_DB',
        schema='WEATHER'
    )
    
    print("\n✅ Connected successfully!")
    
    cursor = conn.cursor()
    
    # Test query
    cursor.execute("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
    result = cursor.fetchone()
    
    print(f"\nConnection Details:")
    print(f"  User: {result[0]}")
    print(f"  Warehouse: {result[1]}")
    print(f"  Database: {result[2]}")
    
    # Check tables
    cursor.execute("SHOW TABLES IN TRAINING_DB.WEATHER")
    tables = cursor.fetchall()
    
    print(f"\n📊 Found {len(tables)} tables in WEATHER schema")
    for table in tables[:5]:
        print(f"  - {table[1]}")
    
    cursor.close()
    conn.close()
    
    print("\n✅ Test completed successfully!")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nTroubleshooting:")
    print("1. Clear browser cookies for snowflakecomputing.com")
    print("2. Try using incognito/private mode")
    print("3. Make sure pop-ups are allowed")