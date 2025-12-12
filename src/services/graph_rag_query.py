from neo4j import GraphDatabase
from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Optional, Literal

import os
from dotenv import load_dotenv
load_dotenv()

# ============================================
# PART 1: Structured Query Models with Instructor
# ============================================

class QueryIntent(BaseModel):
    """Classify user's query intent"""
    intent_type: Literal[
        "find_table",
        "find_owner", 
        "lineage_discovery",
        "column_search",
        "tag_search",
        "relationship_query",
        "general_info"
    ] = Field(description="Type of query the user is asking")
    entities: List[str] = Field(description="Key entities mentioned (table names, column names, teams)")
    search_terms: List[str] = Field(description="Important search keywords")
    requires_traversal: bool = Field(description="Whether query needs graph traversal")

class CypherQuery(BaseModel):
    """Structured Cypher query generation"""
    cypher: str = Field(description="The Cypher query to execute")
    explanation: str = Field(description="Human-readable explanation of what the query does")
    confidence: float = Field(description="Confidence in query correctness", ge=0, le=1)

class DataAssetInfo(BaseModel):
    """Information about a data asset"""
    name: str = Field(description="Name of the table or column")
    type: str = Field(description="Type (Table, Column, etc.)")
    description: Optional[str] = Field(description="Business description")
    owner: Optional[str] = Field(description="Owning team")
    tags: List[str] = Field(default_factory=list, description="Associated tags")
    sensitivity: Optional[str] = Field(description="Data sensitivity level")

class QueryResponse(BaseModel):
    """Structured response to user query"""
    answer: str = Field(description="Natural language answer to the question")
    assets: List[DataAssetInfo] = Field(description="Relevant data assets found")
    cypher_used: Optional[str] = Field(description="Cypher query that was executed")
    suggestions: List[str] = Field(description="Follow-up questions or suggestions")

# ============================================
# PART 2: Graph RAG Query Engine
# ============================================

class GraphRAGQueryEngine:
    """Query engine combining Neo4j graph traversal with LLM reasoning"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, openai_api_key: str):
        # Neo4j connection
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        # LangChain Neo4j Graph
        self.graph = Neo4jGraph(
            url=neo4j_uri,
            username=neo4j_user,
            password=neo4j_password
        )
        
        # Instructor client for structured outputs
        self.instructor_client = instructor.from_openai(OpenAI(api_key=openai_api_key))
        
        # LLM for reasoning
        self.llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0,
            api_key=openai_api_key
        )
        
        # Graph schema for context
        self.schema = self._get_graph_schema()
    
    def _get_graph_schema(self) -> str:
        """Get graph schema for LLM context"""
        return """
        Node Types:
        - Table: Represents database tables
          Properties: name, full_name, catalog, schema, owner_team, tags
        - Column: Represents table columns
          Properties: name, full_name, description, data_type, sensitivity
        - Team: Represents teams that own data
          Properties: name
        - Tag: Represents categorization tags
          Properties: name
        
        Relationship Types:
        - (Team)-[:OWNS]->(Table): Team owns a table
        - (Table)-[:HAS_COLUMN]->(Column): Table contains columns
        - (Table)-[:TAGGED_WITH]->(Tag): Table has tags
        - (Table)-[:RELATED_TO]->(Table): Tables are related
          Properties: type, source_column, target_column, confidence
        """
    
    def understand_query(self, question: str) -> QueryIntent:
        """Use Instructor to understand user intent"""
        
        prompt = f"""
        Analyze this user question about a data catalog:
        "{question}"
        
        Available graph schema:
        {self.schema}
        
        Classify the intent and extract relevant information.
        """
        
        intent = self.instructor_client.chat.completions.create(
            model="gpt-4o",
            response_model=QueryIntent,
            messages=[
                {"role": "system", "content": "You are a data catalog query analyzer."},
                {"role": "user", "content": prompt}
            ]
        )
        
        return intent
    
    def generate_cypher(self, question: str, intent: QueryIntent) -> CypherQuery:
        """Generate Cypher query using Instructor"""
        
        prompt = f"""
        Generate a Cypher query for Neo4j to answer this question:
        "{question}"
        
        Query intent: {intent.intent_type}
        Entities: {intent.entities}
        Search terms: {intent.search_terms}
        
        Graph schema:
        {self.schema}
        
        Generate a safe, read-only Cypher query (use MATCH, not CREATE/DELETE).
        """
        
        cypher_query = self.instructor_client.chat.completions.create(
            model="gpt-4o",
            response_model=CypherQuery,
            messages=[
                {"role": "system", "content": "You are a Cypher query expert."},
                {"role": "user", "content": prompt}
            ]
        )
        
        return cypher_query
    
    def execute_cypher(self, cypher: str) -> List[dict]:
        """Execute Cypher query and return results"""
        with self.driver.session() as session:
            result = session.run(cypher)
            return [record.data() for record in result]
    
    def synthesize_answer(self, question: str, cypher: str, results: List[dict]) -> QueryResponse:
        """Use Instructor to synthesize structured response"""
        
        prompt = f"""
        Original question: "{question}"
        Cypher query executed: {cypher}
        
        Query results:
        {results}
        
        Synthesize a helpful answer with:
        1. Natural language response
        2. List of relevant data assets
        3. Suggestions for follow-up questions
        """
        
        response = self.instructor_client.chat.completions.create(
            model="gpt-4o",
            response_model=QueryResponse,
            messages=[
                {"role": "system", "content": "You are a helpful data catalog assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        
        response.cypher_used = cypher
        return response
    
    def query(self, question: str, verbose: bool = True) -> QueryResponse:
        """Main query method - Graph RAG pipeline"""
        
        if verbose:
            print(f"\n❓ Question: {question}")
            print("=" * 60)
        
        # Step 1: Understand intent
        intent = self.understand_query(question)
        if verbose:
            print(f"🎯 Intent: {intent.intent_type}")
            print(f"📌 Entities: {intent.entities}")
        
        # Step 2: Generate Cypher
        cypher_query = self.generate_cypher(question, intent)
        if verbose:
            print(f"\n💡 Generated Cypher:")
            print(f"   {cypher_query.cypher}")
            print(f"   Confidence: {cypher_query.confidence:.2f}")
        
        # Step 3: Execute query
        results = self.execute_cypher(cypher_query.cypher)
        if verbose:
            print(f"\n📊 Found {len(results)} results")
        
        # Step 4: Synthesize answer
        response = self.synthesize_answer(question, cypher_query.cypher, results)
        
        if verbose:
            print(f"\n✨ Answer:")
            print(f"   {response.answer}")
            
            if response.assets:
                print(f"\n📦 Relevant Assets:")
                for asset in response.assets:
                    print(f"   • {asset.name} ({asset.type})")
                    if asset.owner:
                        print(f"     Owner: {asset.owner}")
                    if asset.tags:
                        print(f"     Tags: {', '.join(asset.tags)}")
            
            if response.suggestions:
                print(f"\n💭 Suggestions:")
                for suggestion in response.suggestions:
                    print(f"   • {suggestion}")
        
        return response
    
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

# ============================================
# PART 3: Pre-built Query Templates
# ============================================

class QuickQueries:
    """Pre-built queries for common patterns"""
    
    @staticmethod
    def find_by_owner(engine: GraphRAGQueryEngine, team_name: str):
        """Find all tables owned by a team"""
        cypher = f"""
        MATCH (team:Team {{name: '{team_name}'}})-[:OWNS]->(t:Table)
        RETURN t.name as table, t.tags as tags
        """
        return engine.execute_cypher(cypher)
    
    @staticmethod
    def find_by_tag(engine: GraphRAGQueryEngine, tag: str):
        """Find all tables with a specific tag"""
        cypher = f"""
        MATCH (t:Table)-[:TAGGED_WITH]->(tag:Tag {{name: '{tag}'}})
        RETURN t.name as table, t.owner_team as owner
        """
        return engine.execute_cypher(cypher)
    
    @staticmethod
    def find_sensitive_columns(engine: GraphRAGQueryEngine):
        """Find all high-sensitivity columns"""
        cypher = """
        MATCH (t:Table)-[:HAS_COLUMN]->(c:Column {sensitivity: 'High'})
        RETURN t.name as table, c.name as column, c.description as description
        ORDER BY t.name
        """
        return engine.execute_cypher(cypher)
    
    @staticmethod
    def find_related_tables(engine: GraphRAGQueryEngine, table_name: str):
        """Find tables related to a specific table"""
        cypher = f"""
        MATCH (t1:Table {{name: '{table_name}'}})-[r:RELATED_TO]->(t2:Table)
        RETURN t2.name as related_table, r.type as relationship_type, 
               r.confidence as confidence
        ORDER BY r.confidence DESC
        """
        return engine.execute_cypher(cypher)
    
    @staticmethod
    def get_table_lineage(engine: GraphRAGQueryEngine, table_name: str):
        """Get complete lineage for a table"""
        cypher = f"""
        MATCH path = (source:Table)-[:RELATED_TO*1..3]->(t:Table {{name: '{table_name}'}})
        RETURN path
        """
        return engine.execute_cypher(cypher)

# ============================================
# PART 4: Interactive Demo
# ============================================

def run_demo():
    """Run interactive demo of Graph RAG"""
    
    print("🌟 Graph RAG Query System")
    print("=" * 60)
    
    # Initialize
    engine = GraphRAGQueryEngine(
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD"),
        openai_api_key=os.getenv("OPENAI_API_KEY")
    )
    
    # Example queries
    example_queries = [
        "What tables does the sales team own?",
        "Show me all tables with customer data",
        "Which tables contain high sensitivity information?",
        "What is the customer_feedback table used for?",
        "How are sales_transactions and customer_feedback related?",
        "Find all columns that might contain personally identifiable information"
    ]
    
    print("\n📝 Example Questions:")
    for i, q in enumerate(example_queries, 1):
        print(f"   {i}. {q}")
    
    print("\n" + "=" * 60)
    print("Type 'quit' to exit, 'quick' for quick queries")
    print("=" * 60)
    
    try:
        while True:
            question = input("\n❓ Your question: ").strip()
            
            if question.lower() == 'quit':
                break
            
            if question.lower() == 'quick':
                print("\n🚀 Quick Queries:")
                print("1. Find by owner")
                print("2. Find by tag")
                print("3. Find sensitive columns")
                print("4. Find related tables")
                
                choice = input("Choose (1-4): ").strip()
                
                if choice == "1":
                    team = input("Team name: ").strip()
                    results = QuickQueries.find_by_owner(engine, team)
                    print(f"\n📊 Results: {results}")
                
                elif choice == "2":
                    tag = input("Tag name: ").strip()
                    results = QuickQueries.find_by_tag(engine, tag)
                    print(f"\n📊 Results: {results}")
                
                elif choice == "3":
                    results = QuickQueries.find_sensitive_columns(engine)
                    print(f"\n📊 Sensitive Columns:")
                    for r in results:
                        print(f"   • {r['table']}.{r['column']}: {r['description']}")
                
                elif choice == "4":
                    table = input("Table name: ").strip()
                    results = QuickQueries.find_related_tables(engine, table)
                    print(f"\n📊 Related Tables:")
                    for r in results:
                        print(f"   • {r['related_table']} ({r['relationship_type']}, "
                              f"confidence: {r['confidence']:.2f})")
                
                continue
            
            if not question:
                continue
            
            # Execute Graph RAG query
            try:
                response = engine.query(question, verbose=True)
            except Exception as e:
                print(f"\n❌ Error: {str(e)}")
    
    finally:
        engine.close()
        print("\n👋 Goodbye!")

# ============================================
# PART 5: Programmatic Usage Example
# ============================================

def programmatic_example():
    """Example of using the engine programmatically"""
    
    engine = GraphRAGQueryEngine(
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD"),
        openai_api_key=os.getenv("OPENAI_API_KEY")
    )
    
    try:
        # Query 1: Find owner
        response1 = engine.query("Who owns the customer_feedback table?", verbose=False)
        print(f"Owner Answer: {response1.answer}")
        
        # Query 2: Search by tag
        response2 = engine.query("Show me all tables tagged with Sales", verbose=False)
        print(f"Sales Tables: {[asset.name for asset in response2.assets]}")
        
        # Query 3: Relationship discovery
        response3 = engine.query(
            "How are the sales and feedback tables connected?", 
            verbose=False
        )
        print(f"Relationship: {response3.answer}")
        
    finally:
        engine.close()

if __name__ == "__main__":
    # Run interactive demo
    run_demo()
    
    # Or use programmatically
    # programmatic_example()