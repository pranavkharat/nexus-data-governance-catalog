# Knowledge Graph Builder with Instructor for Databricks Metadata
# This script extracts metadata from Databricks and builds a Neo4j Knowledge Graph

from databricks.sdk import WorkspaceClient
from neo4j import GraphDatabase
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================
# PART 1: Pydantic Models with Instructor
# ============================================

class Column(BaseModel):
    """Represents a database column with its metadata"""
    name: str = Field(description="Column name")
    description: str = Field(description="Business description of the column")
    data_type: str = Field(description="Data type (STRING, INT, DOUBLE, etc.)")
    sensitivity: str = Field(description="Data sensitivity level (Low, Medium, High)")
    is_key: bool = Field(default=False, description="Whether this is a key field for relationships")

class Table(BaseModel):
    """Represents a database table with its metadata"""
    catalog: str = Field(description="Catalog name")
    schema: str = Field(description="Schema name")
    name: str = Field(description="Table name")
    description: str = Field(description="Business purpose of the table")
    owner_team: str = Field(description="Team that owns this table")
    tags: List[str] = Field(description="Tags for categorization")
    columns: List[Column] = Field(description="List of columns in this table")

class Relationship(BaseModel):
    """Represents a relationship between entities"""
    source_table: str = Field(description="Source table name")
    source_column: str = Field(description="Source column name")
    target_table: str = Field(description="Target table name")
    target_column: str = Field(description="Target column name")
    relationship_type: str = Field(description="Type of relationship (REFERENCES, DERIVES_FROM, etc.)")
    confidence: float = Field(description="Confidence score 0-1", ge=0, le=1)

class KnowledgeGraph(BaseModel):
    """Complete knowledge graph structure"""
    tables: List[Table] = Field(description="All tables in the catalog")
    relationships: List[Relationship] = Field(description="Relationships between tables")
    teams: List[str] = Field(description="List of unique teams owning data")

# ============================================
# PART 2: Databricks Metadata Extractor
# ============================================

class DatabricksMetadataExtractor:
    """Extracts metadata from Databricks Unity Catalog"""
    
    def __init__(self, host: str, token: str, warehouse_id: str):
        self.client = WorkspaceClient(host=host, token=token)
        self.warehouse_id = warehouse_id
    
    def extract_metadata(self, catalog: str, schema: str) -> dict:
        """Extract all metadata from metadata_catalog table"""
        
        query = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            column_name,
            column_description,
            data_type,
            owner_team,
            tags,
            sensitivity_level
        FROM {catalog}.{schema}.metadata_catalog
        ORDER BY table_name, column_name
        """
        
        result = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=query,
            wait_timeout="50s"
        )
        
        # Parse results into structured format
        metadata = {}
        
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                table_name = row[2]
                
                if table_name not in metadata:
                    metadata[table_name] = {
                        'catalog': row[0],
                        'schema': row[1],
                        'name': row[2],
                        'owner_team': row[6],
                        'tags': row[7].split(',') if row[7] else [],
                        'columns': []
                    }
                
                metadata[table_name]['columns'].append({
                    'name': row[3],
                    'description': row[4],
                    'data_type': row[5],
                    'sensitivity': row[8]
                })
        
        return metadata

# ============================================
# PART 3: Instructor-Powered Relationship Detection
# ============================================

class RelationshipDetector:
    """Uses LLM with Instructor to detect relationships between tables"""
    
    def __init__(self, api_key: str):
        # Initialize Instructor with OpenAI
        self.client = instructor.from_openai(OpenAI(api_key=api_key))
    
    def detect_relationships(self, tables_metadata: dict) -> List[Relationship]:
        """Use LLM to intelligently detect relationships between tables"""
        
        # Create a description of all tables for context
        context = self._build_context(tables_metadata)
        
        prompt = f"""
        Analyze the following database tables and identify relationships between them.
        Look for:
        - Foreign key relationships (e.g., customer_id in multiple tables)
        - Derived relationships (e.g., feedback linked to transactions)
        - Common identifiers that link tables
        
        Tables information:
        {context}
        
        Return all relationships you can identify with confidence scores.
        """
        
        # Use Instructor to get structured output
        relationships = self.client.chat.completions.create(
            model="gpt-4o",
            response_model=List[Relationship],
            messages=[
                {"role": "system", "content": "You are a database expert analyzing table relationships."},
                {"role": "user", "content": prompt}
            ],
            max_retries=3
        )
        
        return relationships
    
    def _build_context(self, tables_metadata: dict) -> str:
        """Build context string from metadata"""
        context_parts = []
        
        for table_name, table_info in tables_metadata.items():
            columns = ", ".join([
                f"{col['name']} ({col['data_type']})" 
                for col in table_info['columns']
            ])
            context_parts.append(
                f"Table: {table_name}\n"
                f"Owner: {table_info['owner_team']}\n"
                f"Columns: {columns}\n"
            )
        
        return "\n".join(context_parts)

# ============================================
# PART 4: Neo4j Knowledge Graph Builder
# ============================================

class Neo4jKGBuilder:
    """Builds Knowledge Graph in Neo4j"""
    
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def clear_graph(self):
        """Clear existing graph (for POC purposes)"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("🗑️  Cleared existing graph")
    
    def create_constraints(self):
        """Create uniqueness constraints"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Table) REQUIRE t.full_name IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Column) REQUIRE c.full_name IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (team:Team) REQUIRE team.name IS UNIQUE"
            ]
            
            for constraint in constraints:
                session.run(constraint)
            
            print("✅ Created constraints")
    
    def create_tables(self, tables_metadata: dict):
        """Create Table and Column nodes"""
        with self.driver.session() as session:
            for table_name, table_info in tables_metadata.items():
                # Create Table node
                full_name = f"{table_info['catalog']}.{table_info['schema']}.{table_name}"
                
                session.run("""
                    MERGE (t:Table {full_name: $full_name})
                    SET t.name = $name,
                        t.catalog = $catalog,
                        t.schema = $schema,
                        t.owner_team = $owner_team,
                        t.tags = $tags,
                        t.created_at = datetime()
                """, 
                    full_name=full_name,
                    name=table_name,
                    catalog=table_info['catalog'],
                    schema=table_info['schema'],
                    owner_team=table_info['owner_team'],
                    tags=table_info['tags']
                )
                
                # Create Team node and relationship
                session.run("""
                    MERGE (team:Team {name: $team_name})
                    WITH team
                    MATCH (t:Table {full_name: $full_name})
                    MERGE (team)-[:OWNS]->(t)
                """,
                    team_name=table_info['owner_team'],
                    full_name=full_name
                )
                
                # Create Column nodes and relationships
                for col in table_info['columns']:
                    col_full_name = f"{full_name}.{col['name']}"
                    
                    session.run("""
                        MERGE (c:Column {full_name: $col_full_name})
                        SET c.name = $col_name,
                            c.description = $description,
                            c.data_type = $data_type,
                            c.sensitivity = $sensitivity
                        WITH c
                        MATCH (t:Table {full_name: $table_full_name})
                        MERGE (t)-[:HAS_COLUMN]->(c)
                    """,
                        col_full_name=col_full_name,
                        col_name=col['name'],
                        description=col['description'],
                        data_type=col['data_type'],
                        sensitivity=col['sensitivity'],
                        table_full_name=full_name
                    )
                
                # Create tag relationships
                for tag in table_info['tags']:
                    session.run("""
                        MERGE (tag:Tag {name: $tag_name})
                        WITH tag
                        MATCH (t:Table {full_name: $full_name})
                        MERGE (t)-[:TAGGED_WITH]->(tag)
                    """,
                        tag_name=tag.strip(),
                        full_name=full_name
                    )
        
        print(f"✅ Created {len(tables_metadata)} tables with columns")
    
    def create_relationships(self, relationships: List[Relationship]):
        """Create relationships between tables"""
        with self.driver.session() as session:
            for rel in relationships:
                session.run("""
                    MATCH (t1:Table {name: $source_table})
                    MATCH (t2:Table {name: $target_table})
                    MERGE (t1)-[r:RELATED_TO {
                        type: $rel_type,
                        source_column: $source_col,
                        target_column: $target_col,
                        confidence: $confidence
                    }]->(t2)
                """,
                    source_table=rel.source_table,
                    target_table=rel.target_table,
                    rel_type=rel.relationship_type,
                    source_col=rel.source_column,
                    target_col=rel.target_column,
                    confidence=rel.confidence
                )
        
        print(f"✅ Created {len(relationships)} relationships")
    
    def get_graph_stats(self) -> dict:
        """Get statistics about the graph"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (t:Table) WITH count(t) as tables
                MATCH (c:Column) WITH tables, count(c) as columns
                MATCH (team:Team) WITH tables, columns, count(team) as teams
                MATCH ()-[r:RELATED_TO]->() 
                RETURN tables, columns, teams, count(r) as relationships
            """)
            
            stats = result.single()
            return {
                'tables': stats['tables'],
                'columns': stats['columns'],
                'teams': stats['teams'],
                'relationships': stats['relationships']
            }

# ============================================
# PART 5: Main Orchestration
# ============================================

def main():
    """Main execution flow"""
    
    print("🚀 Starting Knowledge Graph Builder")
    print("=" * 60)
    
    # Configuration
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
    
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    CATALOG = "workspace"  # or "hive_metastore"
    SCHEMA = "sample_data"
    
    # Step 1: Extract metadata from Databricks
    print("\n📊 Step 1: Extracting metadata from Databricks...")
    extractor = DatabricksMetadataExtractor(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
        warehouse_id=DATABRICKS_WAREHOUSE_ID
    )
    
    metadata = extractor.extract_metadata(CATALOG, SCHEMA)
    print(f"✅ Extracted metadata for {len(metadata)} tables")
    
    # Step 2: Detect relationships using Instructor + LLM
    print("\n🔍 Step 2: Detecting relationships with Instructor...")
    detector = RelationshipDetector(api_key=OPENAI_API_KEY)
    relationships = detector.detect_relationships(metadata)
    print(f"✅ Detected {len(relationships)} relationships")
    
    for rel in relationships:
        print(f"   {rel.source_table}.{rel.source_column} -> "
              f"{rel.target_table}.{rel.target_column} "
              f"({rel.relationship_type}, confidence: {rel.confidence:.2f})")
    
    # Step 3: Build Neo4j Knowledge Graph
    print("\n🌐 Step 3: Building Neo4j Knowledge Graph...")
    kg_builder = Neo4jKGBuilder(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD
    )
    
    try:
        kg_builder.clear_graph()  # Clear for POC
        kg_builder.create_constraints()
        kg_builder.create_tables(metadata)
        kg_builder.create_relationships(relationships)
        
        # Get stats
        stats = kg_builder.get_graph_stats()
        
        print("\n" + "=" * 60)
        print("✨ Knowledge Graph Built Successfully!")
        print("=" * 60)
        print(f"📊 Graph Statistics:")
        print(f"   • Tables: {stats['tables']}")
        print(f"   • Columns: {stats['columns']}")
        print(f"   • Teams: {stats['teams']}")
        print(f"   • Relationships: {stats['relationships']}")
        
    finally:
        kg_builder.close()
    
    print("\n🎯 Next Steps:")
    print("   1. Open Neo4j Browser at http://localhost:7474")
    print("   2. Run: MATCH (n) RETURN n LIMIT 50")
    print("   3. Try Graph RAG queries (see next artifact)")

if __name__ == "__main__":
    main()