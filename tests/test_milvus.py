from pymilvus import connections, utility

try:
    connections.connect(
        alias="default",
        host='localhost',
        port='19530'
    )
    print("✅ Connected to Milvus successfully!")
    print(f"📊 Existing collections: {utility.list_collections()}")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")