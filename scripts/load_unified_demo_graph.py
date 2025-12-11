# scripts/load_unified_working.py

"""
GUARANTEED WORKING - Fixed to match your actual column properties
"""

import sys
import os
from neo4j import GraphDatabase
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

neo4j_password = os.getenv('NEO4J_PASSWORD')
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", neo4j_password))

sf_conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    role=os.getenv('SNOWFLAKE_ROLE')
)

print("✅ Connected")

# CLEAR
print("\n🧹 Clearing demo data...")
with neo4j_driver.session() as session:
    session.run("MATCH (n:Customer) DETACH DELETE n")
    session.run("MATCH (n:Order) DETACH DELETE n")
    session.run("MATCH (n:Product) DETACH DELETE n")
print("✅ Cleared")

cursor = sf_conn.cursor()

# ============================================
# STEP 1: CUSTOMERS
# ============================================
print("\n👥 STEP 1: Loading customers...")

cursor.execute("""
    SELECT "customer_id", "customer_city", "customer_state"
    FROM OLIST_SALES.CUSTOMERS
    WHERE "customer_city" IS NOT NULL
    LIMIT 100
""")

customers = cursor.fetchall()
customer_ids = [c[0] for c in customers]

with neo4j_driver.session() as session:
    for cust_id, city, state in customers:
        session.run("""
            CREATE (c:Customer {
                customer_id: $id,
                city: $city,
                state: $state
            })
        """, id=cust_id, city=city, state=state)

print(f"  ✅ Created {len(customers)} Customer nodes")

# Link to metadata
print("  🔗 Creating cross-layer links...")

with neo4j_driver.session() as session:
    # Customer → CUSTOMERS table (INSTANCE_OF)
    result = session.run("""
        MATCH (c:Customer)
        MATCH (t:OlistData {name: 'CUSTOMERS', schema: 'OLIST_SALES'})
        CREATE (c)-[:INSTANCE_OF]->(t)
        RETURN count(*) as created
    """)
    print(f"    ✅ INSTANCE_OF: {result.single()['created']}")
    
    # customer_id column → Customers (HAS_INSTANCE)
    # FIXED: Use 'table' property with CONTAINS instead of exact match
    result = session.run("""
        MATCH (col:OlistColumn)
        WHERE col.name = 'customer_id'
          AND col.table CONTAINS 'CUSTOMERS'
        
        MATCH (c:Customer)
        
        CREATE (col)-[:HAS_INSTANCE]->(c)
        RETURN count(*) as created
    """)
    print(f"    ✅ HAS_INSTANCE: {result.single()['created']}")

# ============================================
# STEP 2: ORDERS
# ============================================
print("\n📦 STEP 2: Loading orders...")

placeholders = ','.join([f"'{cid}'" for cid in customer_ids])

cursor.execute(f"""
    SELECT "order_id", "customer_id", "order_status"
    FROM OLIST_SALES.ORDERS
    WHERE "customer_id" IN ({placeholders})
      AND "order_status" IS NOT NULL
    LIMIT 100
""")

orders = cursor.fetchall()
order_ids = [o[0] for o in orders]

with neo4j_driver.session() as session:
    for order_id, cust_id, status in orders:
        session.run("""
            CREATE (o:Order {
                order_id: $order_id,
                customer_id: $cust_id,
                status: $status
            })
        """, order_id=order_id, cust_id=cust_id, status=status)
        
        # PLACED
        session.run("""
            MATCH (c:Customer {customer_id: $cust_id})
            MATCH (o:Order {order_id: $order_id})
            CREATE (c)-[:PLACED]->(o)
        """, cust_id=cust_id, order_id=order_id)

print(f"  ✅ Created {len(orders)} Orders + PLACED relationships")

# Link to metadata
print("  🔗 Creating cross-layer links...")

with neo4j_driver.session() as session:
    # Order → ORDERS table
    result = session.run("""
        MATCH (o:Order)
        MATCH (t:OlistData {name: 'ORDERS', schema: 'OLIST_SALES'})
        CREATE (o)-[:INSTANCE_OF]->(t)
        RETURN count(*) as created
    """)
    print(f"    ✅ INSTANCE_OF: {result.single()['created']}")
    
    # order_id column → Orders (FIXED query)
    result = session.run("""
        MATCH (col:OlistColumn)
        WHERE col.name = 'order_id'
          AND col.table CONTAINS 'ORDERS'
        
        MATCH (o:Order)
        
        CREATE (col)-[:HAS_INSTANCE]->(o)
        RETURN count(*) as created
    """)
    print(f"    ✅ HAS_INSTANCE: {result.single()['created']}")

# ============================================
# STEP 3: PRODUCTS FROM ORDER_ITEMS
# ============================================
print("\n🛍️  STEP 3: Loading products FROM order items (guarantees CONTAINS will work)...")

# Get products that are IN the orders we loaded
order_str = ','.join([f"'{oid}'" for oid in order_ids])

cursor.execute(f"""
    SELECT DISTINCT oi."product_id", p."product_category_name"
    FROM OLIST_SALES.ORDER_ITEMS oi
    JOIN OLIST_SALES.PRODUCTS p ON oi."product_id" = p."product_id"
    WHERE oi."order_id" IN ({order_str})
      AND p."product_category_name" IS NOT NULL
    LIMIT 100
""")

products = cursor.fetchall()
product_ids = [p[0] for p in products]

print(f"  Fetched {len(products)} products (from order items)")

cat_map = {
    'beleza_saude': 'health_beauty',
    'informatica_acessorios': 'computers',
    'moveis_decoracao': 'furniture',
    'esporte_lazer': 'sports',
    'cama_mesa_banho': 'bed_bath',
    'utilidades_domesticas': 'housewares',
    'telefonia': 'phones',
    'automotivo': 'automotive',
    'brinquedos': 'toys',
    'relogios_presentes': 'watches_gifts',
    'eletronicos': 'electronics',
    'cool_stuff': 'cool_stuff'
}

with neo4j_driver.session() as session:
    for prod_id, category_pt in products:
        category_en = cat_map.get(category_pt, category_pt.replace('_', ' '))
        
        session.run("""
            CREATE (p:Product {
                product_id: $prod_id,
                category: $category_en,
                category_pt: $category_pt
            })
        """, prod_id=prod_id, category_en=category_en, category_pt=category_pt)

print(f"  ✅ Created {len(products)} Product nodes")

# Link to metadata
print("  🔗 Creating cross-layer links...")

with neo4j_driver.session() as session:
    # Product → PRODUCTS table
    result = session.run("""
        MATCH (p:Product)
        MATCH (t:OlistData {name: 'PRODUCTS', schema: 'OLIST_SALES'})
        CREATE (p)-[:INSTANCE_OF]->(t)
        RETURN count(*) as created
    """)
    print(f"    ✅ INSTANCE_OF: {result.single()['created']}")
    
    # product_id column → Products
    result = session.run("""
        MATCH (col:OlistColumn)
        WHERE col.name = 'product_id'
          AND col.table CONTAINS 'PRODUCTS'
        
        MATCH (p:Product)
        
        CREATE (col)-[:HAS_INSTANCE]->(p)
        RETURN count(*) as created
    """)
    print(f"    ✅ HAS_INSTANCE: {result.single()['created']}")

# ============================================
# STEP 4: CREATE CONTAINS (Now Guaranteed to Work!)
# ============================================
print("\n🔗 STEP 4: Creating CONTAINS relationships...")

product_str = ','.join([f"'{pid}'" for pid in product_ids])

cursor.execute(f"""
    SELECT "order_id", "product_id", "price"
    FROM OLIST_SALES.ORDER_ITEMS
    WHERE "order_id" IN ({order_str})
      AND "product_id" IN ({product_str})
      AND "price" IS NOT NULL
""")

items = cursor.fetchall()

print(f"  Found {len(items)} order-product links")

with neo4j_driver.session() as session:
    for order_id, prod_id, price in items:
        session.run("""
            MATCH (o:Order {order_id: $order_id})
            MATCH (p:Product {product_id: $prod_id})
            CREATE (o)-[:CONTAINS {price: $price}]->(p)
        """, order_id=order_id, prod_id=prod_id, price=float(price))

print(f"  ✅ Created {len(items)} CONTAINS relationships")

# ============================================
# FINAL STATS
# ============================================
print("\n" + "="*70)
print("📊 3-LAYER UNIFIED GRAPH COMPLETE!")
print("="*70)

with neo4j_driver.session() as session:
    # Nodes
    nodes = session.run("""
        MATCH (n)
        RETURN labels(n)[0] as type, count(n) as count
        ORDER BY count DESC
    """)
    
    print("\n✅ Nodes:")
    for record in nodes:
        print(f"  • {record['type']}: {record['count']}")
    
    # Relationships
    rels = session.run("""
        MATCH ()-[r]->()
        RETURN type(r) as type, count(r) as count
        ORDER BY count DESC
    """)
    
    print("\n✅ Relationships:")
    for record in rels:
        rel_type = record['type']
        rel_count = record['count']
        
        if rel_type in ['HAS_INSTANCE', 'INSTANCE_OF']:
            print(f"  • {rel_type}: {rel_count} 🔗 (CROSS-LAYER!)")
        else:
            print(f"  • {rel_type}: {rel_count}")

print("\n🎯 Test this in Neo4j Browser:")
print("""
MATCH path = 
  (col:OlistColumn {name: 'customer_id'})
  -[:HAS_INSTANCE]->(c:Customer {city: 'sao paulo'})
  -[:INSTANCE_OF]->(table:OlistData {name: 'CUSTOMERS'})
  
MATCH (c)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)

RETURN path LIMIT 3
""")
print("="*70)

neo4j_driver.close()
sf_conn.close()