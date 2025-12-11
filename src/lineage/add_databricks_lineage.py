# add_databricks_lineage.py
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", os.getenv('NEO4J_PASSWORD'))
)

print("\n" + "="*60)
print("🔗 ADDING DATABRICKS LINEAGE")
print("="*60)

with driver.session() as session:
    # Create lineage: customer_feedback derives from sales_transactions
    result = session.run("""
        MATCH (feedback:FederatedTable {table_name: 'customer_feedback', source: 'databricks'})
        MATCH (sales:FederatedTable {table_name: 'sales_transactions', source: 'databricks'})
        MERGE (feedback)-[r:DERIVES_FROM]->(sales)
        SET r.lineage_type = 'FOREIGN_KEY',
            r.confidence = 1.0,
            r.join_column = 'transaction_id',
            r.source = 'databricks',
            r.discovered_at = datetime(),
            r.description = 'Feedback linked to transactions via transaction_id'
        RETURN feedback.table_name AS from_table, 
               sales.table_name AS to_table,
               r.lineage_type AS type
    """)
    
    record = result.single()
    if record:
        print(f"\n   ✅ Created: {record['from_table']} → {record['to_table']}")
        print(f"   📋 Type: {record['type']}")
        print(f"   🔑 Join Column: transaction_id")
        print(f"   🎯 Confidence: 100%")
    else:
        print("\n   ⚠️ Tables not found. Run federation first.")

    # Verify total lineage count
    count = session.run("""
        MATCH ()-[r:DERIVES_FROM]->() 
        RETURN count(r) AS total
    """).single()['total']
    
    print(f"\n   📊 Total DERIVES_FROM edges: {count}")

driver.close()
print("\n✅ Done!")