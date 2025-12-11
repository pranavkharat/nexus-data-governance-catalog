# owner.py 
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", os.getenv('NEO4J_PASSWORD'))
)

with driver.session() as session:
    result = session.run("""
        MATCH (t:OlistData)
        SET t.owner = CASE t.schema
            WHEN 'OLIST_SALES' THEN 'data_engineering_team'
            WHEN 'OLIST_MARKETING' THEN 'marketing_analytics_team'
            WHEN 'OLIST_ANALYTICS' THEN 'business_intelligence_team'
            ELSE 'unknown'
        END
        RETURN t.schema AS schema, t.name AS name, t.owner AS owner
    """)
    
    print("✅ Ownership added:")
    for record in result:
        print(f"   {record['schema']}.{record['name']} → {record['owner']}")

driver.close()
print("\n✅ Done!")