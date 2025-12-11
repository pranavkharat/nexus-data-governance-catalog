# src/graphrag/few_shot_examples.py

"""
Few-Shot Examples for Text-to-Cypher Generation
UPDATED: Now includes Databricks and Cross-Source patterns

Based on YOUR ACTUAL Neo4j schema:
- Layer 2: Customer, Order, Product (sample data)
- Layer 3: OlistData, OlistColumn (Snowflake metadata)
- Layer 4: FederatedTable, FederatedColumn (Databricks metadata)
- Cross-source: SIMILAR_TO relationships
"""

# ============================================
# SAMPLE DATA QUERIES (Layer 2 - Customer, Order, Product nodes)
# ============================================

SAMPLE_DATA_EXAMPLES = """
Question: How many customers from São Paulo?
Cypher: MATCH (c:Customer) WHERE toLower(c.city) = 'sao paulo' RETURN count(c) as count

Question: Show me customers from Rio de Janeiro
Cypher: MATCH (c:Customer) WHERE toLower(c.city) CONTAINS 'rio' RETURN c.customer_id, c.city, c.state LIMIT 10

Question: Which customers placed the most orders?
Cypher: MATCH (c:Customer)-[:PLACED]->(o:Order) RETURN c.customer_id, count(o) as order_count ORDER BY order_count DESC LIMIT 10

Question: How many orders are delivered?
Cypher: MATCH (o:Order) WHERE toLower(o.status) = 'delivered' RETURN count(o) as count

Question: Show me delivered orders
Cypher: MATCH (o:Order) WHERE toLower(o.status) = 'delivered' RETURN o.order_id, o.customer_id, o.status LIMIT 10

Question: What customer ID purchased furniture?
Cypher: MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product) WHERE toLower(p.category_pt) CONTAINS 'moveis' OR toLower(p.category) = 'furniture' RETURN DISTINCT c.customer_id, p.category LIMIT 10

Question: Show me customers who bought computers
Cypher: MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product) WHERE toLower(p.category_pt) = 'informatica_acessorios' OR toLower(p.category) CONTAINS 'computer' RETURN c.customer_id, c.city LIMIT 10

Question: List products in sports category
Cypher: MATCH (p:Product) WHERE toLower(p.category_pt) = 'esporte_lazer' OR toLower(p.category) = 'sports' RETURN p.product_id, p.category, p.category_pt LIMIT 10

Question: Which products are in furniture category?
Cypher: MATCH (p:Product) WHERE toLower(p.category) = 'furniture' OR toLower(p.category_pt) CONTAINS 'moveis' RETURN p.product_id, p.category, p.category_pt LIMIT 10
"""

# ============================================
# SNOWFLAKE METADATA QUERIES (Layer 3 - OlistData nodes)
# ============================================

METADATA_EXAMPLES = """
Question: Which tables have the most rows?
Cypher: MATCH (t:OlistData) RETURN t.schema + '.' + t.name as table, t.row_count as rows ORDER BY t.row_count DESC LIMIT 5

Question: Find tables in OLIST_SALES schema
Cypher: MATCH (t:OlistData) WHERE t.schema = 'OLIST_SALES' RETURN t.name, t.row_count ORDER BY t.name

Question: Show duplicate tables
Cypher: MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData) RETURN t1.schema + '.' + t1.name as source, t2.schema + '.' + t2.name as duplicate, d.confidence as confidence

Question: Which tables contain customer information?
Cypher: MATCH (t:OlistData) WHERE toLower(t.name) CONTAINS 'customer' RETURN t.schema + '.' + t.name as table, t.row_count as rows

Question: Show table lineage
Cypher: MATCH (target:OlistData)-[r:DERIVES_FROM]->(source:OlistData) RETURN target.schema + '.' + target.name as derived_table, source.schema + '.' + source.name as source_table, r.lineage_type as type, r.confidence as confidence

Question: What does CLIENT_DATA derive from?
Cypher: MATCH (t:OlistData {name: 'CLIENT_DATA'})-[r:DERIVES_FROM]->(source:OlistData) RETURN source.schema + '.' + source.name as source_table, r.lineage_type as type, r.confidence as confidence

Question: Show columns in CUSTOMERS table
Cypher: MATCH (t:OlistData {name: 'CUSTOMERS'})-[:HAS_COLUMN]->(c:OlistColumn) RETURN c.name as column_name, c.data_type as type, c.ordinal_position as position ORDER BY c.ordinal_position
"""

# ============================================
# DATABRICKS METADATA QUERIES (Layer 4 - FederatedTable nodes)
# ============================================

DATABRICKS_EXAMPLES = """
Question: Show me Databricks tables
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' RETURN t.full_name as table, t.row_count as rows, t.owner as owner, t.column_count as columns ORDER BY t.full_name

Question: What columns are in sales_transactions?
Cypher: MATCH (t:FederatedTable {table_name: 'sales_transactions'})-[:HAS_COLUMN]->(c:FederatedColumn) RETURN c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, c.sensitivity as sensitivity, c.position as position ORDER BY c.position

Question: What columns are in customer_feedback?
Cypher: MATCH (t:FederatedTable {table_name: 'customer_feedback'})-[:HAS_COLUMN]->(c:FederatedColumn) RETURN c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, c.sensitivity as sensitivity ORDER BY c.position

Question: Who owns sales_transactions?
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' AND t.table_name = 'sales_transactions' RETURN t.full_name as table, t.owner as owner, t.row_count as rows

Question: Who owns the Databricks tables?
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' RETURN t.table_name as table, t.owner as owner ORDER BY t.owner

Question: Which columns have high sensitivity?
Cypher: MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) WHERE t.source = 'databricks' AND c.sensitivity IN ['High', 'Critical', 'high', 'critical'] RETURN t.full_name + '.' + c.name as column, c.sensitivity as sensitivity, c.data_type as type

Question: Show sensitive columns in Databricks
Cypher: MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) WHERE t.source = 'databricks' AND c.sensitivity IS NOT NULL RETURN t.table_name as table, c.name as column, c.sensitivity as sensitivity ORDER BY c.sensitivity, t.table_name

Question: List all Databricks columns
Cypher: MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) WHERE t.source = 'databricks' RETURN t.table_name as table, c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type ORDER BY t.table_name, c.position

Question: How many columns in each Databricks table?
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:FederatedColumn) RETURN t.table_name as table, count(c) as column_count, t.row_count as rows ORDER BY t.table_name
"""

# ============================================
# CROSS-SOURCE QUERIES (SIMILAR_TO relationships)
# ============================================

CROSS_SOURCE_EXAMPLES = """
Question: Find cross-source matches
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) RETURN db.full_name as databricks_table, sf.schema + '.' + sf.name as snowflake_table, r.score as similarity_score, r.confidence as confidence ORDER BY r.score DESC

Question: Which Databricks tables are similar to Snowflake?
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) WHERE db.source = 'databricks' RETURN db.table_name as databricks, sf.name as snowflake, round(r.score * 100, 1) as similarity_pct ORDER BY r.score DESC LIMIT 10

Question: What Snowflake tables match sales_transactions?
Cypher: MATCH (db:FederatedTable {table_name: 'sales_transactions'})-[r:SIMILAR_TO]->(sf:OlistData) RETURN sf.schema + '.' + sf.name as snowflake_table, round(r.score * 100, 1) as similarity_pct, r.confidence as confidence ORDER BY r.score DESC

Question: What Snowflake tables match customer_feedback?
Cypher: MATCH (db:FederatedTable {table_name: 'customer_feedback'})-[r:SIMILAR_TO]->(sf:OlistData) RETURN sf.schema + '.' + sf.name as snowflake_table, round(r.score * 100, 1) as similarity_pct ORDER BY r.score DESC

Question: Find tables similar to CUSTOMERS across platforms
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData {name: 'CUSTOMERS'}) RETURN db.full_name as databricks_table, round(r.score * 100, 1) as similarity_pct, r.semantic_score as semantic ORDER BY r.score DESC

Question: Show all SIMILAR_TO relationships
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) RETURN db.table_name as source, sf.name as target, r.score as score, r.confidence as confidence ORDER BY r.score DESC LIMIT 20

Question: Which tables have cross-source duplicates?
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) WHERE r.score >= 0.30 RETURN db.table_name as databricks, sf.schema + '.' + sf.name as snowflake, round(r.score * 100, 1) as match_pct ORDER BY r.score DESC

Question: High confidence cross-source matches
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) WHERE r.confidence = 'high' OR r.score >= 0.5 RETURN db.full_name as databricks, sf.schema + '.' + sf.name as snowflake, r.score as score
"""

# ============================================
# GOVERNANCE QUERIES (SHACL validation context)
# ============================================

GOVERNANCE_EXAMPLES = """
Question: Which tables have no owner?
Cypher: MATCH (t:FederatedTable) WHERE t.owner IS NULL OR trim(t.owner) = '' RETURN t.full_name as table, t.source as platform

Question: Tables missing lineage
Cypher: MATCH (t:OlistData) WHERE t.schema IN ['OLIST_MARKETING', 'OLIST_ANALYTICS'] AND NOT EXISTS { MATCH (t)-[:DERIVES_FROM]->(:OlistData) } RETURN t.schema + '.' + t.name as table

Question: Databricks tables without columns
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' AND NOT EXISTS { MATCH (t)-[:HAS_COLUMN]->(:FederatedColumn) } RETURN t.full_name as table

Question: Low confidence duplicates
Cypher: MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData) WHERE d.confidence < 0.5 RETURN t1.name as source, t2.name as duplicate, d.confidence as confidence
"""

# ============================================
# CATEGORY MAPPINGS (English ↔ Portuguese)
# ============================================

CATEGORY_MAPPINGS = {
    # English → Portuguese
    'furniture': 'moveis_decoracao',
    'computers': 'informatica_acessorios',
    'sports': 'esporte_lazer',
    'bed_bath': 'cama_mesa_banho',
    'toys': 'brinquedos',
    'health_beauty': 'beleza_saude',
    'watches_gifts': 'relogios_presentes',
    'housewares': 'utilidades_domesticas',
    'automotive': 'automotivo',
    'phones': 'telefonia',
    'pet_shop': 'pet_shop',
    
    # Portuguese → English (reverse)
    'moveis_decoracao': 'furniture',
    'informatica_acessorios': 'computers',
    'esporte_lazer': 'sports',
    'cama_mesa_banho': 'bed_bath',
    'brinquedos': 'toys',
    'beleza_saude': 'health_beauty',
}

# ============================================
# COMBINED PROMPT TEMPLATES
# ============================================

def get_cypher_prompt_with_examples(query_type: str = 'sample_data') -> str:
    """
    Get appropriate prompt template based on query type.
    
    Args:
        query_type: One of 'sample_data', 'metadata', 'databricks', 'cross_source', 'governance'
    """
    
    base_rules = """CRITICAL RULES:
1. OUTPUT ONLY VALID CYPHER - no explanations, no markdown, no "Here is..."
2. Always use LIMIT (default 10, max 20)
3. Use toLower() for ALL text matching
4. Property names are case-sensitive - use exact names from schema
"""
    
    if query_type == 'sample_data':
        return f"""You are a Neo4j Cypher expert for an e-commerce database.

{base_rules}

SCHEMA (Sample Data - Layer 2):
- Node: Customer (customer_id, city, state)
- Node: Order (order_id, customer_id, status)
- Node: Product (product_id, category, category_pt)
- Relationship: [:PLACED] (Customer→Order)
- Relationship: [:CONTAINS] (Order→Product)

EXAMPLES:
{SAMPLE_DATA_EXAMPLES}

Question: {{question}}
Cypher:"""
    
    elif query_type == 'metadata':
        return f"""You are a Neo4j Cypher expert for a data catalog.

{base_rules}

SCHEMA (Snowflake Metadata - Layer 3):
- Node: OlistData (name, schema, database, row_count, column_count, owner)
- Node: OlistColumn (name, data_type, ordinal_position)
- Relationship: [:HAS_COLUMN] (OlistData→OlistColumn)
- Relationship: [:OLIST_DUPLICATE] (OlistData→OlistData)
- Relationship: [:DERIVES_FROM] (OlistData→OlistData)

EXAMPLES:
{METADATA_EXAMPLES}

Question: {{question}}
Cypher:"""
    
    elif query_type == 'databricks':
        return f"""You are a Neo4j Cypher expert for a federated data catalog.

{base_rules}

SCHEMA (Databricks Metadata - Layer 4):
- Node: FederatedTable (full_name, table_name, source, schema, row_count, column_count, owner)
  - Filter Databricks: WHERE t.source = 'databricks'
- Node: FederatedColumn (name, data_type, position, sensitivity, nullable)
- Relationship: [:HAS_COLUMN] (FederatedTable→FederatedColumn)
- Relationship: [:FROM_SOURCE] (FederatedTable→DataSource)

IMPORTANT:
- data_type has prefix "ColumnTypeName." - use replace() to clean
- sensitivity values: 'Low', 'Medium', 'High', 'Critical'

EXAMPLES:
{DATABRICKS_EXAMPLES}

Question: {{question}}
Cypher:"""
    
    elif query_type == 'cross_source':
        return f"""You are a Neo4j Cypher expert for cross-platform data discovery.

{base_rules}

SCHEMA (Cross-Source):
- Node: FederatedTable (Databricks tables where source='databricks')
- Node: OlistData (Snowflake tables)
- Relationship: [:SIMILAR_TO] (FederatedTable→OlistData)
  - Properties: score (0-1), confidence ('low'/'medium'/'high'), semantic_score

EXAMPLES:
{CROSS_SOURCE_EXAMPLES}

Question: {{question}}
Cypher:"""
    
    elif query_type == 'governance':
        return f"""You are a Neo4j Cypher expert for data governance validation.

{base_rules}

SCHEMA:
- OlistData (Snowflake tables)
- FederatedTable (all federated tables including Databricks)
- FederatedColumn (Databricks columns with sensitivity)
- Relationships: DERIVES_FROM, OLIST_DUPLICATE, HAS_COLUMN, SIMILAR_TO

EXAMPLES:
{GOVERNANCE_EXAMPLES}

Question: {{question}}
Cypher:"""
    
    else:
        # Default combined prompt
        return f"""You are a Neo4j Cypher expert for a multi-source data catalog.

{base_rules}

AVAILABLE NODE TYPES:
1. Sample Data: Customer, Order, Product
2. Snowflake Metadata: OlistData, OlistColumn
3. Databricks Metadata: FederatedTable (source='databricks'), FederatedColumn
4. Cross-Source: SIMILAR_TO relationships between FederatedTable and OlistData

EXAMPLES (Sample Data):
{SAMPLE_DATA_EXAMPLES}

EXAMPLES (Metadata):
{METADATA_EXAMPLES}

EXAMPLES (Databricks):
{DATABRICKS_EXAMPLES}

Question: {{question}}
Cypher:"""


def get_all_examples() -> str:
    """Return all examples combined for reference."""
    return f"""
{'='*60}
SAMPLE DATA EXAMPLES (Customer, Order, Product)
{'='*60}
{SAMPLE_DATA_EXAMPLES}

{'='*60}
SNOWFLAKE METADATA EXAMPLES (OlistData)
{'='*60}
{METADATA_EXAMPLES}

{'='*60}
DATABRICKS EXAMPLES (FederatedTable, FederatedColumn)
{'='*60}
{DATABRICKS_EXAMPLES}

{'='*60}
CROSS-SOURCE EXAMPLES (SIMILAR_TO)
{'='*60}
{CROSS_SOURCE_EXAMPLES}

{'='*60}
GOVERNANCE EXAMPLES
{'='*60}
{GOVERNANCE_EXAMPLES}
"""


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    print("Few-Shot Examples Module")
    print("=" * 60)
    
    # Show example counts
    examples = {
        'sample_data': SAMPLE_DATA_EXAMPLES.count('Question:'),
        'metadata': METADATA_EXAMPLES.count('Question:'),
        'databricks': DATABRICKS_EXAMPLES.count('Question:'),
        'cross_source': CROSS_SOURCE_EXAMPLES.count('Question:'),
        'governance': GOVERNANCE_EXAMPLES.count('Question:'),
    }
    
    total = sum(examples.values())
    
    print(f"\n📚 Example Counts:")
    for category, count in examples.items():
        print(f"   {category}: {count} examples")
    print(f"   {'─'*30}")
    print(f"   TOTAL: {total} examples")
    
    # Show sample prompts
    print(f"\n📝 Sample Databricks Prompt:")
    print("-" * 60)
    prompt = get_cypher_prompt_with_examples('databricks')
    print(prompt[:500] + "...")
    
    print("\n✅ Module loaded successfully!")