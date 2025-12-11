# src/graphrag/langchain_graphrag.py

"""
LangChain + Ollama Text-to-Cypher
UPDATED: Now supports sample_data, metadata, databricks, and cross_source query types
"""

from langchain_community.graphs import Neo4jGraph
from langchain.chains import GraphCypherQAChain
from langchain_ollama import ChatOllama
from langchain.prompts import PromptTemplate
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional
import json
import re

# Import our few-shot examples
from src.graphrag.few_shot_examples import (
    get_cypher_prompt_with_examples,
    CATEGORY_MAPPINGS,
    SAMPLE_DATA_EXAMPLES,
    METADATA_EXAMPLES,
    DATABRICKS_EXAMPLES,
    CROSS_SOURCE_EXAMPLES,
    GOVERNANCE_EXAMPLES
)

load_dotenv()


class LangChainGraphRAGEngine:
    """
    Production LangChain engine with:
    - Multi-query-type support (sample_data, metadata, databricks, cross_source)
    - Proper few-shot examples for each type
    - Improved answer generation
    - Retry logic for empty results
    """
    
    def __init__(self):
        print("\n🚀 Initializing LangChain GraphRAG Engine (Multi-Type)...")
        
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        
        self.graph = Neo4jGraph(
            url="bolt://localhost:7687",
            username="neo4j",
            password=neo4j_password,
            refresh_schema=True
        )
        print("✅ Connected to Neo4j")
        
        # Main LLM for Cypher generation (temperature=0 for consistency)
        self.cypher_llm = ChatOllama(
            model="llama3.1",
            base_url="http://localhost:11434",
            temperature=0.0
        )
        
        # Separate LLM for answer generation (slightly higher temp for natural language)
        self.answer_llm = ChatOllama(
            model="llama3.1",
            base_url="http://localhost:11434",
            temperature=0.5
        )
        print("✅ Ollama initialized (dual LLM setup)")
        
        # Create prompt templates for each query type
        self.prompt_templates = {
            'sample_data': self._create_sample_data_prompt(),
            'metadata': self._create_metadata_prompt(),
            'databricks': self._create_databricks_prompt(),
            'cross_source': self._create_cross_source_prompt(),
        }
        
        # Create chains for each query type
        self.chains = {}
        for query_type, prompt in self.prompt_templates.items():
            self.chains[query_type] = GraphCypherQAChain.from_llm(
                llm=self.cypher_llm,
                graph=self.graph,
                cypher_prompt=prompt,
                verbose=True,
                return_intermediate_steps=True,
                allow_dangerous_requests=True
            )
        
        print(f"✅ Created {len(self.chains)} LangChain chains (sample_data, metadata, databricks, cross_source)\n")
        
        # Extended category mappings (English ↔ Portuguese)
        self.categories = {
            'furniture': ['furniture', 'moveis_decoracao', 'moveis'],
            'computer': ['computers', 'informatica_acessorios', 'informatica'],
            'sport': ['sports', 'esporte_lazer', 'esporte'],
            'health': ['health_beauty', 'beleza_saude', 'beleza'],
            'beauty': ['health_beauty', 'beleza_saude', 'beleza'],
            'watch': ['watches_gifts', 'relogios_presentes', 'relogio'],
            'gift': ['watches_gifts', 'relogios_presentes', 'presente'],
            'toy': ['toys', 'brinquedos'],
            'phone': ['phones', 'telefonia', 'celular'],
            'bed': ['bed_bath', 'cama_mesa_banho', 'cama'],
            'bath': ['bed_bath', 'cama_mesa_banho', 'banho'],
            'pet': ['pet_shop'],
            'auto': ['automotive', 'automotivo'],
            'electronic': ['electronics', 'eletronicos', 'informatica_acessorios'],
        }

    # ================================================================
    # PROMPT TEMPLATES FOR EACH QUERY TYPE
    # ================================================================
    
    def _create_sample_data_prompt(self) -> PromptTemplate:
        """Prompt for sample data queries (Customer, Order, Product)"""
        template = """You are a Neo4j Cypher expert for an e-commerce database.

SCHEMA:
{schema}

CRITICAL RULES:
1. Node labels: Customer, Order, Product
2. Customer properties: customer_id, city, state
3. Order properties: order_id, customer_id, status (delivered/shipped/canceled/invoiced)
4. Product properties: product_id, category, category_pt
5. Relationships: [:PLACED] (Customer→Order), [:CONTAINS] (Order→Product)
6. Always use LIMIT (default 10, max 20)
7. Use toLower() for ALL text matching
8. For categories, check BOTH category AND category_pt fields
9. OUTPUT ONLY VALID CYPHER - no explanations, no markdown

EXAMPLES:
Question: How many customers from São Paulo?
Cypher: MATCH (c:Customer) WHERE toLower(c.city) = 'sao paulo' RETURN count(c) as count

Question: Show me customers from Rio de Janeiro
Cypher: MATCH (c:Customer) WHERE toLower(c.city) CONTAINS 'rio' RETURN c.customer_id, c.city, c.state LIMIT 10

Question: Which customers placed the most orders?
Cypher: MATCH (c:Customer)-[:PLACED]->(o:Order) RETURN c.customer_id, count(o) as order_count ORDER BY order_count DESC LIMIT 10

Question: How many orders are delivered?
Cypher: MATCH (o:Order) WHERE toLower(o.status) = 'delivered' RETURN count(o) as count

Question: What customer purchased furniture?
Cypher: MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product) WHERE toLower(p.category_pt) CONTAINS 'moveis' OR toLower(p.category) = 'furniture' RETURN DISTINCT c.customer_id, p.category LIMIT 10

Question: Show products in electronics category
Cypher: MATCH (p:Product) WHERE toLower(p.category_pt) CONTAINS 'informatica' OR toLower(p.category) CONTAINS 'computer' OR toLower(p.category) CONTAINS 'electronic' RETURN p.product_id, p.category, p.category_pt LIMIT 10

Question: {question}
Cypher:"""
        return PromptTemplate(input_variables=["schema", "question"], template=template)
    
    def _create_metadata_prompt(self) -> PromptTemplate:
        """Prompt for Snowflake metadata queries (OlistData, OlistColumn)"""
        template = """You are a Neo4j Cypher expert for a data catalog.

SCHEMA:
{schema}

CRITICAL RULES:
1. Node labels: OlistData (tables), OlistColumn (columns)
2. OlistData properties: name, schema, database, row_count, column_count, owner
3. OlistColumn properties: name, data_type, ordinal_position
4. Relationships: [:HAS_COLUMN] (OlistData→OlistColumn), [:OLIST_DUPLICATE], [:DERIVES_FROM]
5. Always use LIMIT (default 10, max 20)
6. Use toLower() for text matching
7. OUTPUT ONLY VALID CYPHER - no explanations, no markdown

EXAMPLES:
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

Question: Show columns in CUSTOMERS table
Cypher: MATCH (t:OlistData {{name: 'CUSTOMERS'}})-[:HAS_COLUMN]->(c:OlistColumn) RETURN c.name as column_name, c.data_type as type, c.ordinal_position as position ORDER BY c.ordinal_position

Question: {question}
Cypher:"""
        return PromptTemplate(input_variables=["schema", "question"], template=template)
    
    def _create_databricks_prompt(self) -> PromptTemplate:
        """Prompt for Databricks metadata queries (FederatedTable, FederatedColumn)"""
        template = """You are a Neo4j Cypher expert for a federated data catalog with Databricks.

SCHEMA:
{schema}

CRITICAL RULES:
1. Node labels: FederatedTable, FederatedColumn
2. FederatedTable properties: full_name, table_name, source, schema, row_count, column_count, owner
   - IMPORTANT: Filter Databricks tables with: WHERE t.source = 'databricks'
3. FederatedColumn properties: name, data_type, position, sensitivity, nullable
   - data_type has prefix "ColumnTypeName." - use replace(c.data_type, 'ColumnTypeName.', '') to clean
   - sensitivity values: 'Low', 'Medium', 'High', 'Critical'
4. Relationships: [:HAS_COLUMN] (FederatedTable→FederatedColumn)
5. Always use LIMIT unless showing all columns (then LIMIT 50)
6. OUTPUT ONLY VALID CYPHER - no explanations, no markdown

EXAMPLES:
Question: Show me Databricks tables
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' RETURN t.full_name as table, t.row_count as rows, t.owner as owner, t.column_count as columns ORDER BY t.full_name

Question: What columns are in sales_transactions?
Cypher: MATCH (t:FederatedTable {{table_name: 'sales_transactions'}})-[:HAS_COLUMN]->(c:FederatedColumn) RETURN c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, c.sensitivity as sensitivity, c.position as position ORDER BY c.position

Question: What columns are in customer_feedback?
Cypher: MATCH (t:FederatedTable {{table_name: 'customer_feedback'}})-[:HAS_COLUMN]->(c:FederatedColumn) RETURN c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, c.sensitivity as sensitivity ORDER BY c.position

Question: Who owns sales_transactions?
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' AND t.table_name = 'sales_transactions' RETURN t.full_name as table, t.owner as owner, t.row_count as rows

Question: Who owns the Databricks tables?
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' RETURN t.table_name as table, t.owner as owner ORDER BY t.owner

Question: Which columns have high sensitivity?
Cypher: MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) WHERE t.source = 'databricks' AND c.sensitivity IN ['High', 'Critical', 'high', 'critical'] RETURN t.full_name + '.' + c.name as column, c.sensitivity as sensitivity, replace(c.data_type, 'ColumnTypeName.', '') as type

Question: Show sensitive columns in Databricks
Cypher: MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) WHERE t.source = 'databricks' AND c.sensitivity IS NOT NULL RETURN t.table_name as table, c.name as column, c.sensitivity as sensitivity ORDER BY c.sensitivity, t.table_name

Question: List all Databricks columns
Cypher: MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) WHERE t.source = 'databricks' RETURN t.table_name as table, c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, c.sensitivity as sensitivity ORDER BY t.table_name, c.position LIMIT 50

Question: How many columns in each Databricks table?
Cypher: MATCH (t:FederatedTable) WHERE t.source = 'databricks' OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:FederatedColumn) RETURN t.table_name as table, count(c) as column_count, t.row_count as rows ORDER BY t.table_name

Question: {question}
Cypher:"""
        return PromptTemplate(input_variables=["schema", "question"], template=template)
    
    def _create_cross_source_prompt(self) -> PromptTemplate:
        """Prompt for cross-source similarity queries (SIMILAR_TO relationships)"""
        template = """You are a Neo4j Cypher expert for cross-platform data discovery.

SCHEMA:
{schema}

CRITICAL RULES:
1. Cross-source matches use [:SIMILAR_TO] relationship
2. Direction: (FederatedTable)-[:SIMILAR_TO]->(OlistData) means Databricks→Snowflake
3. SIMILAR_TO properties: score (0-1), confidence ('low'/'medium'/'high'), semantic_score
4. FederatedTable: Databricks tables (filter with source='databricks')
5. OlistData: Snowflake tables
6. Always ORDER BY score DESC for relevance
7. OUTPUT ONLY VALID CYPHER - no explanations, no markdown

EXAMPLES:
Question: Find cross-source matches
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) RETURN db.full_name as databricks_table, sf.schema + '.' + sf.name as snowflake_table, r.score as similarity_score, r.confidence as confidence ORDER BY r.score DESC

Question: Which Databricks tables are similar to Snowflake?
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) WHERE db.source = 'databricks' RETURN db.table_name as databricks, sf.name as snowflake, round(r.score * 100, 1) as similarity_pct ORDER BY r.score DESC LIMIT 10

Question: What Snowflake tables match sales_transactions?
Cypher: MATCH (db:FederatedTable {{table_name: 'sales_transactions'}})-[r:SIMILAR_TO]->(sf:OlistData) RETURN sf.schema + '.' + sf.name as snowflake_table, round(r.score * 100, 1) as similarity_pct, r.confidence as confidence ORDER BY r.score DESC

Question: What Snowflake tables match customer_feedback?
Cypher: MATCH (db:FederatedTable {{table_name: 'customer_feedback'}})-[r:SIMILAR_TO]->(sf:OlistData) RETURN sf.schema + '.' + sf.name as snowflake_table, round(r.score * 100, 1) as similarity_pct ORDER BY r.score DESC

Question: Find tables similar to CUSTOMERS across platforms
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData {{name: 'CUSTOMERS'}}) RETURN db.full_name as databricks_table, round(r.score * 100, 1) as similarity_pct, r.semantic_score as semantic ORDER BY r.score DESC

Question: Show all SIMILAR_TO relationships
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) RETURN db.table_name as source, sf.name as target, r.score as score, r.confidence as confidence ORDER BY r.score DESC LIMIT 20

Question: Which tables have cross-source duplicates?
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) WHERE r.score >= 0.30 RETURN db.table_name as databricks, sf.schema + '.' + sf.name as snowflake, round(r.score * 100, 1) as match_pct ORDER BY r.score DESC

Question: High confidence cross-source matches
Cypher: MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) WHERE r.confidence = 'high' OR r.score >= 0.5 RETURN db.full_name as databricks, sf.schema + '.' + sf.name as snowflake, r.score as score

Question: {question}
Cypher:"""
        return PromptTemplate(input_variables=["schema", "question"], template=template)

    # ================================================================
    # MAIN QUERY METHOD (UPDATED)
    # ================================================================
    
    def query(self, nl_question: str, top_k: int = 10, query_type: str = 'sample_data') -> Dict:
        """
        Main query method with multi-type support.
        
        Args:
            nl_question: Natural language question
            top_k: Maximum results to return
            query_type: One of 'sample_data', 'metadata', 'databricks', 'cross_source'
        
        Returns:
            Dict with question, query_type, generated_cypher, neo4j_results, nl_answer, success
        """
        print(f"\n{'='*70}")
        print(f"🤖 LANGCHAIN QUERY")
        print(f"   Question: '{nl_question}'")
        print(f"   Type: {query_type}")
        print(f"{'='*70}")
        
        # For sample_data, try manual patterns first (existing logic)
        if query_type == 'sample_data':
            manual_result = self._try_manual_cypher(nl_question)
            if manual_result:
                print("✅ Manual pattern matched")
                return manual_result
        
        # Use appropriate chain for query type
        chain = self.chains.get(query_type, self.chains['sample_data'])
        print(f"⚙️ Using LangChain chain: {query_type}")
        
        try:
            result = chain.invoke({"query": nl_question})
            
            generated_cypher = result['intermediate_steps'][0]['query']
            neo4j_results = result['intermediate_steps'][1]['context']
            
            # Clean up the cypher for display
            generated_cypher = self._clean_cypher(generated_cypher)
            
            print(f"📝 Generated: {generated_cypher[:100]}...")
            print(f"💾 Results: {len(neo4j_results)} rows")
            
            # If empty results and sample_data, try query relaxation
            if (not neo4j_results or len(neo4j_results) == 0) and query_type == 'sample_data':
                print("⚠️ Empty results - attempting query relaxation...")
                relaxed_result = self._try_relaxed_query(nl_question, generated_cypher)
                if relaxed_result:
                    return relaxed_result
            
            # Generate natural language answer
            nl_answer = self._generate_answer(nl_question, neo4j_results, generated_cypher, query_type)
            
            return {
                'question': nl_question,
                'query_type': f'langchain_{query_type}',
                'results': [],
                'generated_cypher': generated_cypher,
                'neo4j_results': neo4j_results,
                'nl_answer': nl_answer,
                'success': True
            }
            
        except Exception as e:
            print(f"❌ LangChain failed: {e}")
            
            # Try fallback for sample_data queries
            if query_type == 'sample_data':
                fallback = self._fallback_query(nl_question)
                if fallback:
                    return fallback
            
            # Try manual Cypher for databricks/cross_source
            if query_type in ['databricks', 'cross_source']:
                manual_fallback = self._try_manual_databricks_cypher(nl_question, query_type)
                if manual_fallback:
                    return manual_fallback
            
            return {
                'question': nl_question,
                'query_type': f'langchain_{query_type}',
                'results': [],
                'generated_cypher': '',
                'neo4j_results': [],
                'error': str(e),
                'nl_answer': f"I couldn't process this query. Error: {str(e)[:100]}",
                'success': False
            }
    
    def _clean_cypher(self, cypher: str) -> str:
        """Clean up LLM-generated Cypher"""
        # Remove markdown code blocks
        cypher = cypher.replace('```cypher', '').replace('```', '')
        
        # Remove common LLM preambles
        prefixes_to_remove = [
            'Here is the Cypher query:',
            'The Cypher query is:',
            'Cypher:',
            'Here is',
        ]
        for prefix in prefixes_to_remove:
            if cypher.strip().lower().startswith(prefix.lower()):
                cypher = cypher.strip()[len(prefix):]
        
        # Take only the first statement
        cypher = cypher.split(';')[0].strip()
        
        return cypher.strip()
    
    # ================================================================
    # MANUAL CYPHER FALLBACKS FOR DATABRICKS/CROSS-SOURCE
    # ================================================================
    
    def _try_manual_databricks_cypher(self, question: str, query_type: str) -> Optional[Dict]:
        """Manual Cypher patterns for Databricks and cross-source queries"""
        q = question.lower()
        cypher = None
        
        if query_type == 'databricks':
            # List all columns
            if 'all' in q and 'column' in q:
                cypher = """MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) 
WHERE t.source = 'databricks' 
RETURN t.table_name as table, c.name as column, 
       replace(c.data_type, 'ColumnTypeName.', '') as type, 
       c.sensitivity as sensitivity 
ORDER BY t.table_name, c.position"""
            
            # Columns in specific table
            elif 'column' in q:
                if 'sales_transactions' in q:
                    cypher = """MATCH (t:FederatedTable {table_name: 'sales_transactions'})-[:HAS_COLUMN]->(c:FederatedColumn) 
RETURN c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, 
       c.sensitivity as sensitivity, c.position as position 
ORDER BY c.position"""
                elif 'customer_feedback' in q:
                    cypher = """MATCH (t:FederatedTable {table_name: 'customer_feedback'})-[:HAS_COLUMN]->(c:FederatedColumn) 
RETURN c.name as column, replace(c.data_type, 'ColumnTypeName.', '') as type, 
       c.sensitivity as sensitivity, c.position as position 
ORDER BY c.position"""
            
            # Show tables
            elif 'table' in q or 'databricks' in q:
                cypher = """MATCH (t:FederatedTable) WHERE t.source = 'databricks' 
RETURN t.full_name as table, t.row_count as rows, t.owner as owner, t.column_count as columns 
ORDER BY t.full_name"""
            
            # Sensitivity queries
            elif 'sensitiv' in q:
                if 'high' in q or 'critical' in q:
                    cypher = """MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) 
WHERE t.source = 'databricks' AND c.sensitivity IN ['High', 'Critical'] 
RETURN t.table_name as table, c.name as column, c.sensitivity as sensitivity 
ORDER BY c.sensitivity, t.table_name"""
                else:
                    cypher = """MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn) 
WHERE t.source = 'databricks' AND c.sensitivity IS NOT NULL 
RETURN t.table_name as table, c.name as column, c.sensitivity as sensitivity 
ORDER BY c.sensitivity, t.table_name"""
            
            # Owner queries
            elif 'owner' in q or 'who owns' in q:
                cypher = """MATCH (t:FederatedTable) WHERE t.source = 'databricks' 
RETURN t.table_name as table, t.owner as owner, t.row_count as rows 
ORDER BY t.owner"""
        
        elif query_type == 'cross_source':
            # All cross-source matches
            if 'all' in q or 'find' in q or 'show' in q:
                cypher = """MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) 
RETURN db.table_name as databricks, sf.schema + '.' + sf.name as snowflake, 
       round(r.score * 100, 1) as similarity_pct, r.confidence as confidence 
ORDER BY r.score DESC LIMIT 20"""
            
            # Specific table matches
            elif 'sales_transactions' in q:
                cypher = """MATCH (db:FederatedTable {table_name: 'sales_transactions'})-[r:SIMILAR_TO]->(sf:OlistData) 
RETURN sf.schema + '.' + sf.name as snowflake_table, round(r.score * 100, 1) as similarity_pct, 
       r.confidence as confidence 
ORDER BY r.score DESC"""
            
            elif 'customer_feedback' in q:
                cypher = """MATCH (db:FederatedTable {table_name: 'customer_feedback'})-[r:SIMILAR_TO]->(sf:OlistData) 
RETURN sf.schema + '.' + sf.name as snowflake_table, round(r.score * 100, 1) as similarity_pct, 
       r.confidence as confidence 
ORDER BY r.score DESC"""
            
            # High confidence matches
            elif 'high' in q and 'confidence' in q:
                cypher = """MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData) 
WHERE r.confidence = 'high' OR r.score >= 0.5 
RETURN db.table_name as databricks, sf.name as snowflake, r.score as score 
ORDER BY r.score DESC"""
        
        if cypher:
            return self._execute_and_format(cypher, question, query_type)
        
        return None

    # ================================================================
    # EXISTING METHODS (UPDATED)
    # ================================================================
    
    def _try_manual_cypher(self, question: str) -> Optional[Dict]:
        """Comprehensive pattern matching for sample_data queries"""
        q = question.lower()
        print(f"🔍 Analyzing: '{question}'")
        
        # PATTERN 1: Product + Category queries
        if 'product' in q or 'category' in q:
            for keyword, search_values in self.categories.items():
                if keyword in q:
                    print(f"🔧 PRODUCT/CATEGORY: '{keyword}'")
                    conditions = " OR ".join([
                        f"toLower(p.category) CONTAINS '{v}'" for v in search_values
                    ] + [
                        f"toLower(p.category_pt) CONTAINS '{v}'" for v in search_values
                    ])
                    cypher = f"""MATCH (p:Product) WHERE {conditions}
RETURN p.product_id, p.category, p.category_pt LIMIT 20"""
                    return self._execute_and_format(cypher, question)
        
        # PATTERN 2: Customer purchase + category
        purchase_words = ['purchase', 'bought', 'buy', 'ordered']
        if any(w in q for w in purchase_words) and 'customer' in q:
            for keyword, search_values in self.categories.items():
                if keyword in q:
                    print(f"🔧 CUSTOMER PURCHASE: '{keyword}'")
                    conditions = " OR ".join([
                        f"toLower(p.category) CONTAINS '{v}'" for v in search_values
                    ] + [
                        f"toLower(p.category_pt) CONTAINS '{v}'" for v in search_values
                    ])
                    cypher = f"""MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
WHERE {conditions}
RETURN DISTINCT c.customer_id, c.city, p.category, p.category_pt LIMIT 20"""
                    return self._execute_and_format(cypher, question)
        
        # PATTERN 3: Customer from city
        city_match = re.search(r'(?:from|in)\s+([a-zA-Z\s]+?)(?:\?|$|,)', q)
        if city_match and 'customer' in q:
            city = city_match.group(1).strip()
            print(f"🔧 CITY QUERY: '{city}'")
            if 'how many' in q or 'count' in q:
                cypher = f"MATCH (c:Customer) WHERE toLower(c.city) CONTAINS '{city}' RETURN count(c) as count"
            else:
                cypher = f"MATCH (c:Customer) WHERE toLower(c.city) CONTAINS '{city}' RETURN c.customer_id, c.city, c.state LIMIT 20"
            return self._execute_and_format(cypher, question)
        
        # PATTERN 4: Order status queries
        statuses = ['delivered', 'shipped', 'canceled', 'invoiced', 'processing']
        for status in statuses:
            if status in q:
                print(f"🔧 ORDER STATUS: '{status}'")
                if 'how many' in q or 'count' in q:
                    cypher = f"MATCH (o:Order) WHERE toLower(o.status) = '{status}' RETURN count(o) as count"
                else:
                    cypher = f"MATCH (o:Order) WHERE toLower(o.status) = '{status}' RETURN o.order_id, o.customer_id, o.status LIMIT 20"
                return self._execute_and_format(cypher, question)
        
        # PATTERN 5: Most orders / top customers
        if ('most order' in q or 'top customer' in q) and 'customer' in q:
            print("🔧 TOP CUSTOMERS BY ORDERS")
            cypher = "MATCH (c:Customer)-[:PLACED]->(o:Order) RETURN c.customer_id, c.city, count(o) as orders ORDER BY orders DESC LIMIT 20"
            return self._execute_and_format(cypher, question)
        
        # PATTERN 6: Count queries
        if 'how many' in q:
            if 'customer' in q:
                print("🔧 COUNT CUSTOMERS")
                cypher = "MATCH (c:Customer) RETURN count(c) as count"
                return self._execute_and_format(cypher, question)
            elif 'order' in q:
                print("🔧 COUNT ORDERS")
                cypher = "MATCH (o:Order) RETURN count(o) as count"
                return self._execute_and_format(cypher, question)
            elif 'product' in q:
                print("🔧 COUNT PRODUCTS")
                cypher = "MATCH (p:Product) RETURN count(p) as count"
                return self._execute_and_format(cypher, question)
        
        # PATTERN 7: Show all / list queries
        if 'show' in q or 'list' in q:
            if 'customer' in q:
                cypher = "MATCH (c:Customer) RETURN c.customer_id, c.city, c.state LIMIT 20"
                return self._execute_and_format(cypher, question)
            elif 'order' in q:
                cypher = "MATCH (o:Order) RETURN o.order_id, o.customer_id, o.status LIMIT 20"
                return self._execute_and_format(cypher, question)
            elif 'product' in q:
                cypher = "MATCH (p:Product) RETURN p.product_id, p.category, p.category_pt LIMIT 20"
                return self._execute_and_format(cypher, question)
        
        print("⚠️ No manual pattern matched")
        return None
    
    def _try_relaxed_query(self, question: str, original_cypher: str) -> Optional[Dict]:
        """Try relaxing query constraints when original returns empty"""
        q = question.lower()
        
        for keyword, search_values in self.categories.items():
            if keyword in q:
                print(f"🔄 Relaxing category search for '{keyword}'")
                
                cypher = f"""MATCH (p:Product)
WHERE toLower(p.category) CONTAINS '{search_values[0]}' 
   OR toLower(p.category_pt) CONTAINS '{search_values[0]}'
RETURN p.product_id, p.category, p.category_pt LIMIT 20"""
                
                results = self._execute_manual_cypher(cypher)
                if results and len(results) > 0:
                    nl_answer = self._generate_answer(question, results, cypher)
                    return {
                        'question': question,
                        'query_type': 'langchain_relaxed',
                        'results': [],
                        'generated_cypher': cypher,
                        'neo4j_results': results,
                        'nl_answer': nl_answer,
                        'success': True
                    }
        
        return None
    
    def _fallback_query(self, question: str) -> Optional[Dict]:
        """Last resort fallback for failed queries"""
        q = question.lower()
        
        if 'customer' in q:
            cypher = "MATCH (c:Customer) RETURN c.customer_id, c.city, c.state LIMIT 10"
        elif 'order' in q:
            cypher = "MATCH (o:Order) RETURN o.order_id, o.customer_id, o.status LIMIT 10"
        elif 'product' in q:
            cypher = "MATCH (p:Product) RETURN p.product_id, p.category, p.category_pt LIMIT 10"
        else:
            return None
        
        try:
            results = self._execute_manual_cypher(cypher)
            nl_answer = f"I found {len(results)} sample records. Here are some examples from the database."
            
            return {
                'question': question,
                'query_type': 'langchain_fallback',
                'results': [],
                'generated_cypher': cypher,
                'neo4j_results': results,
                'nl_answer': nl_answer,
                'success': True
            }
        except:
            return None
    
    def _execute_and_format(self, cypher: str, question: str, query_type: str = 'sample_data') -> Optional[Dict]:
        """Execute Cypher and format result"""
        try:
            results = self._execute_manual_cypher(cypher)
            print(f"   ✅ Executed: {len(results)} rows")
            
            nl_answer = self._generate_answer(question, results, cypher, query_type)
            
            return {
                'question': question,
                'query_type': f'langchain_{query_type}',
                'results': [],
                'generated_cypher': cypher,
                'neo4j_results': results,
                'nl_answer': nl_answer,
                'success': True
            }
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None
    
    def _execute_manual_cypher(self, cypher: str) -> List[Dict]:
        """Execute Cypher directly"""
        with self.graph._driver.session() as session:
            result = session.run(cypher)
            return result.data()
    
    def _generate_answer(self, question: str, results: List[Dict], cypher: str = "", query_type: str = 'sample_data') -> str:
        """Generate natural language answer with query-type awareness"""
        
        if not results or len(results) == 0:
            if query_type == 'databricks':
                return "No matching Databricks data found. Check if FederatedTable nodes exist."
            elif query_type == 'cross_source':
                return "No cross-source matches found. Run duplicate detection first."
            return "No matching data found in the sample dataset (100 rows per entity type)."
        
        # Handle count queries
        if len(results) == 1:
            keys = list(results[0].keys())
            for key in keys:
                if 'count' in key.lower():
                    count = results[0][key]
                    entity = self._infer_entity(question, query_type)
                    return f"Found **{count:,}** {entity} matching your query."
        
        # For list queries
        num_results = len(results)
        sample_data = json.dumps(results[:5], indent=2, default=str)
        
        # Query-type specific context
        if query_type == 'databricks':
            context = "This data comes from Databricks (FederatedTable/FederatedColumn nodes)."
        elif query_type == 'cross_source':
            context = "These are cross-platform similarity matches between Databricks and Snowflake."
        elif query_type == 'metadata':
            context = "This is Snowflake metadata from the OlistData nodes."
        else:
            context = "This is sample e-commerce data."
        
        prompt = f"""You are a helpful data assistant. Generate a clear, specific answer.

QUESTION: {question}
CONTEXT: {context}

DATA FOUND ({num_results} rows, showing first 5):
{sample_data}

INSTRUCTIONS:
1. Start with the count: "Found X results..."
2. Describe what was found specifically
3. Mention 2-3 specific examples from the data
4. Keep response under 100 words
5. Be factual - only state what's in the data

ANSWER:"""
        
        try:
            response = self.answer_llm.invoke(prompt)
            answer = response.content.strip()
            
            bad_phrases = ["don't know", "cannot", "no information", "not able", "i'm sorry"]
            if any(phrase in answer.lower() for phrase in bad_phrases):
                return self._template_answer(question, results, query_type)
            
            return answer
            
        except Exception as e:
            print(f"⚠️ Answer generation failed: {e}")
            return self._template_answer(question, results, query_type)
    
    def _template_answer(self, question: str, results: List[Dict], query_type: str = 'sample_data') -> str:
        """Reliable template-based answer when LLM fails"""
        n = len(results)
        entity = self._infer_entity(question, query_type)
        
        if results:
            first = results[0]
            sample_info = ", ".join([f"{k}: {v}" for k, v in list(first.items())[:3]])
            return f"Found **{n}** {entity}. Example: {sample_info}"
        
        return f"Found {n} {entity} matching your query."
    
    def _infer_entity(self, question: str, query_type: str = 'sample_data') -> str:
        """Infer entity type from question and query type"""
        q = question.lower()
        
        if query_type == 'databricks':
            if 'column' in q: return 'Databricks columns'
            if 'table' in q: return 'Databricks tables'
            if 'sensitiv' in q: return 'sensitive columns'
            return 'Databricks records'
        
        if query_type == 'cross_source':
            return 'cross-source matches'
        
        if query_type == 'metadata':
            if 'column' in q: return 'columns'
            if 'table' in q: return 'tables'
            if 'duplicate' in q: return 'duplicate pairs'
            return 'metadata records'
        
        # sample_data
        if 'customer' in q: return 'customers'
        if 'order' in q: return 'orders'
        if 'product' in q: return 'products'
        if 'seller' in q: return 'sellers'
        return 'records'
    
    def test_connection(self):
        """Test connections"""
        print("\n🔍 Testing Connections...")
        try:
            with self.graph._driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
                count = result.single()['count']
                print(f"✅ Neo4j: {count} nodes")
        except Exception as e:
            print(f"❌ Neo4j: {e}")
        
        try:
            test = self.cypher_llm.invoke("Say OK")
            print(f"✅ Ollama: {test.content[:20]}")
        except Exception as e:
            print(f"❌ Ollama: {e}")


# ================================================================
# TESTING
# ================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🧪 TESTING MULTI-TYPE LANGCHAIN ENGINE")
    print("="*70)
    
    engine = LangChainGraphRAGEngine()
    engine.test_connection()
    
    # Test queries by type
    test_cases = [
        # Sample data queries
        ("which customer purchased furniture?", "sample_data"),
        ("how many delivered orders?", "sample_data"),
        
        # Metadata queries
        ("which tables have the most rows?", "metadata"),
        ("show duplicate tables", "metadata"),
        
        # Databricks queries
        ("list all databricks columns", "databricks"),
        ("what columns are in sales_transactions?", "databricks"),
        ("which columns have high sensitivity?", "databricks"),
        ("who owns the databricks tables?", "databricks"),
        
        # Cross-source queries
        ("find cross-source matches", "cross_source"),
        ("what snowflake tables match sales_transactions?", "cross_source"),
    ]
    
    for query, query_type in test_cases:
        print(f"\n{'='*70}")
        print(f"TEST: '{query}' (type: {query_type})")
        result = engine.query(query, query_type=query_type)
        print(f"✅ Success: {result['success']}")
        print(f"📝 Cypher: {result.get('generated_cypher', 'N/A')[:80]}...")
        print(f"📊 Results: {len(result.get('neo4j_results', []))} rows")
        print(f"💬 Answer: {result['nl_answer'][:100]}...")
    
    print("\n✅ Testing complete!")