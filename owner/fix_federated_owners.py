# fix_federated_owners.py
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", os.getenv('NEO4J_PASSWORD'))
)

with driver.session() as session:
    # Copy owner from OlistData to FederatedTable via MIRRORS relationship
    result = session.run("""
        MATCH (ft:FederatedTable)-[:MIRRORS]->(od:OlistData)
        WHERE od.owner IS NOT NULL
        SET ft.owner = od.owner
        RETURN ft.table_name AS table, ft.owner AS owner
    """)
    
    print("✅ Federated table owners updated:")
    for record in result:
        print(f"   {record['table']} → {record['owner']}")

driver.close()
print("\n✅ Done! Refresh the Federation tab.")