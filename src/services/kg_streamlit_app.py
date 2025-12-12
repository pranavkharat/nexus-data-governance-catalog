import streamlit as st
from neo4j import GraphDatabase
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Optional
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

# ============================================
# Page Configuration
# ============================================

st.set_page_config(
    page_title="Knowledge Graph Data Catalog",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# Models (Reuse from previous artifacts)
# ============================================

class DataAssetInfo(BaseModel):
    name: str = Field(description="Name of the table or column")
    type: str = Field(description="Type (Table, Column, etc.)")
    description: Optional[str] = Field(description="Business description")
    owner: Optional[str] = Field(description="Owning team")
    tags: List[str] = Field(default_factory=list, description="Associated tags")
    sensitivity: Optional[str] = Field(description="Data sensitivity level")

class QueryResponse(BaseModel):
    answer: str = Field(description="Natural language answer to the question")
    assets: List[DataAssetInfo] = Field(description="Relevant data assets found")
    suggestions: List[str] = Field(description="Follow-up questions or suggestions")

# ============================================
# Neo4j Connection
# ============================================

@st.cache_resource
def get_neo4j_driver():
    """Get Neo4j driver connection"""
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(
            os.getenv("NEO4J_USER", "neo4j"),
            os.getenv("NEO4J_PASSWORD")
        )
    )

@st.cache_resource
def get_instructor_client():
    """Get Instructor client"""
    return instructor.from_openai(
        OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    )

# ============================================
# Graph Queries
# ============================================

def get_all_tables():
    """Fetch all tables from the knowledge graph"""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Table)
            OPTIONAL MATCH (team:Team)-[:OWNS]->(t)
            OPTIONAL MATCH (t)-[:TAGGED_WITH]->(tag:Tag)
            RETURN t.name as name, 
                   t.catalog as catalog,
                   t.schema as schema,
                   team.name as owner,
                   collect(DISTINCT tag.name) as tags
            ORDER BY t.name
        """)
        return [record.data() for record in result]

def get_table_details(table_name: str):
    """Get detailed information about a specific table"""
    driver = get_neo4j_driver()
    with driver.session() as session:
        # Get columns
        columns_result = session.run("""
            MATCH (t:Table {name: $table_name})-[:HAS_COLUMN]->(c:Column)
            RETURN c.name as name,
                   c.description as description,
                   c.data_type as data_type,
                   c.sensitivity as sensitivity
            ORDER BY c.name
        """, table_name=table_name)
        
        columns = [record.data() for record in columns_result]
        
        # Get relationships
        rel_result = session.run("""
            MATCH (t:Table {name: $table_name})-[r:RELATED_TO]->(t2:Table)
            RETURN t2.name as related_table,
                   r.type as relationship_type,
                   r.confidence as confidence
            ORDER BY r.confidence DESC
        """, table_name=table_name)
        
        relationships = [record.data() for record in rel_result]
        
        return columns, relationships

def get_graph_statistics():
    """Get overall statistics about the knowledge graph"""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Table) WITH count(t) as tables
            MATCH (c:Column) WITH tables, count(c) as columns
            MATCH (team:Team) WITH tables, columns, count(team) as teams
            MATCH ()-[r:RELATED_TO]->() WITH tables, columns, teams, count(r) as relationships
            MATCH (tag:Tag) WITH tables, columns, teams, relationships, count(tag) as tags
            RETURN tables, columns, teams, relationships, tags
        """)
        return result.single().data()

def search_by_keyword(keyword: str):
    """Search tables and columns by keyword"""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Table)
            WHERE toLower(t.name) CONTAINS toLower($keyword)
            OPTIONAL MATCH (team:Team)-[:OWNS]->(t)
            RETURN DISTINCT t.name as name, 'Table' as type, team.name as owner
            
            UNION
            
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE toLower(c.name) CONTAINS toLower($keyword) 
               OR toLower(c.description) CONTAINS toLower($keyword)
            OPTIONAL MATCH (team:Team)-[:OWNS]->(t)
            RETURN DISTINCT c.name as name, 'Column' as type, team.name as owner
            
            LIMIT 20
        """, keyword=keyword)
        return [record.data() for record in result]

# ============================================
# AI Query Function
# ============================================

def ai_query(question: str) -> QueryResponse:
    """Query using AI with Instructor for structured output"""
    client = get_instructor_client()
    driver = get_neo4j_driver()
    
    # Get graph schema for context
    with driver.session() as session:
        tables_result = session.run("""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            RETURN t.name as name, 
                   collect(DISTINCT {name: c.name, type: c.data_type}) as columns
        """)
        tables_info = [record.data() for record in tables_result]
    
    context = f"Available tables: {tables_info}"
    
    # Generate response using Instructor
    response = client.chat.completions.create(
        model="gpt-4o",
        response_model=QueryResponse,
        messages=[
            {
                "role": "system", 
                "content": f"You are a helpful data catalog assistant. {context}"
            },
            {
                "role": "user",
                "content": question
            }
        ]
    )
    
    return response

# ============================================
# Streamlit UI
# ============================================

def main():
    # Header
    st.title("🌐 Knowledge Graph Data Catalog")
    st.markdown("**Powered by Neo4j + Instructor + GPT-4**")
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Graph Statistics")
        
        try:
            stats = get_graph_statistics()
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Tables", stats['tables'])
                st.metric("Teams", stats['teams'])
            
            with col2:
                st.metric("Columns", stats['columns'])
                st.metric("Tags", stats['tags'])
            
            st.metric("Relationships", stats['relationships'])
            
        except Exception as e:
            st.error(f"Error connecting to Neo4j: {str(e)}")
            st.info("Make sure Neo4j is running and credentials are correct in .env")
        
        st.markdown("---")
        st.header("🔗 Quick Links")
        st.markdown("- [Neo4j Browser](http://localhost:7474)")
        st.markdown("- [Documentation](#)")
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔍 AI Search", 
        "📚 Browse Tables", 
        "🔎 Keyword Search",
        "📈 Analytics"
    ])
    
    # TAB 1: AI Search
    with tab1:
        st.header("🤖 AI-Powered Search")
        st.markdown("Ask questions about your data in natural language")
        
        # Example questions
        with st.expander("💡 Example Questions"):
            examples = [
                "What tables does the sales team own?",
                "Show me all tables with customer data",
                "Which columns contain sensitive information?",
                "How are sales and feedback tables related?",
                "Who should I contact for access to revenue data?"
            ]
            for example in examples:
                if st.button(example, key=f"ex_{example}"):
                    st.session_state['ai_question'] = example
        
        # Query input
        question = st.text_input(
            "Your question:",
            value=st.session_state.get('ai_question', ''),
            placeholder="e.g., What tables contain customer information?"
        )
        
        if st.button("🔍 Search", type="primary"):
            if question:
                with st.spinner("Analyzing your question..."):
                    try:
                        response = ai_query(question)
                        
                        # Display answer
                        st.success("✅ Answer")
                        st.write(response.answer)
                        
                        # Display assets
                        if response.assets:
                            st.markdown("### 📦 Relevant Data Assets")
                            for asset in response.assets:
                                with st.expander(f"{asset.name} ({asset.type})"):
                                    if asset.description:
                                        st.write(f"**Description:** {asset.description}")
                                    if asset.owner:
                                        st.write(f"**Owner:** {asset.owner}")
                                    if asset.tags:
                                        st.write(f"**Tags:** {', '.join(asset.tags)}")
                                    if asset.sensitivity:
                                        st.write(f"**Sensitivity:** {asset.sensitivity}")
                        
                        # Display suggestions
                        if response.suggestions:
                            st.markdown("### 💭 Follow-up Questions")
                            for suggestion in response.suggestions:
                                st.info(suggestion)
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("Please enter a question")
    
    # TAB 2: Browse Tables
    with tab2:
        st.header("📚 Browse All Tables")
        
        try:
            tables = get_all_tables()
            
            if tables:
                # Convert to DataFrame for display
                df = pd.DataFrame(tables)
                df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if x else '')
                
                st.dataframe(
                    df,
                    column_config={
                        "name": "Table Name",
                        "catalog": "Catalog",
                        "schema": "Schema",
                        "owner": "Owner Team",
                        "tags": "Tags"
                    },
                    hide_index=True,
                    use_container_width=True
                )
                
                # Table details
                st.markdown("---")
                st.subheader("🔍 Table Details")
                
                selected_table = st.selectbox(
                    "Select a table to view details:",
                    options=[t['name'] for t in tables]
                )
                
                if selected_table:
                    columns, relationships = get_table_details(selected_table)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("#### 📋 Columns")
                        if columns:
                            cols_df = pd.DataFrame(columns)
                            st.dataframe(
                                cols_df,
                                column_config={
                                    "name": "Column Name",
                                    "data_type": "Type",
                                    "sensitivity": "Sensitivity",
                                    "description": "Description"
                                },
                                hide_index=True,
                                use_container_width=True
                            )
                        else:
                            st.info("No columns found")
                    
                    with col2:
                        st.markdown("#### 🔗 Relationships")
                        if relationships:
                            for rel in relationships:
                                st.write(f"**→ {rel['related_table']}**")
                                st.caption(
                                    f"Type: {rel['relationship_type']} | "
                                    f"Confidence: {rel['confidence']:.0%}"
                                )
                                st.markdown("---")
                        else:
                            st.info("No relationships found")
            
            else:
                st.warning("No tables found in the knowledge graph")
        
        except Exception as e:
            st.error(f"Error loading tables: {str(e)}")
    
    # TAB 3: Keyword Search
    with tab3:
        st.header("🔎 Keyword Search")
        st.markdown("Search for tables and columns by keyword")
        
        search_term = st.text_input(
            "Search keyword:",
            placeholder="e.g., customer, sales, feedback"
        )
        
        if search_term:
            try:
                results = search_by_keyword(search_term)
                
                if results:
                    st.success(f"Found {len(results)} results")
                    
                    results_df = pd.DataFrame(results)
                    st.dataframe(
                        results_df,
                        column_config={
                            "name": "Name",
                            "type": "Type",
                            "owner": "Owner"
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.info("No results found")
            
            except Exception as e:
                st.error(f"Search error: {str(e)}")
    
    # TAB 4: Analytics
    with tab4:
        st.header("📈 Data Catalog Analytics")
        
        try:
            # Tables by owner
            driver = get_neo4j_driver()
            with driver.session() as session:
                owner_result = session.run("""
                    MATCH (team:Team)-[:OWNS]->(t:Table)
                    RETURN team.name as team, count(t) as table_count
                    ORDER BY table_count DESC
                """)
                
                owner_data = [record.data() for record in owner_result]
                
                if owner_data:
                    st.subheader("📊 Tables by Owner Team")
                    owner_df = pd.DataFrame(owner_data)
                    st.bar_chart(owner_df.set_index('team'))
                
                # Sensitivity distribution
                sens_result = session.run("""
                    MATCH (c:Column)
                    RETURN c.sensitivity as sensitivity, count(c) as count
                    ORDER BY count DESC
                """)
                
                sens_data = [record.data() for record in sens_result]
                
                if sens_data:
                    st.subheader("🔒 Data Sensitivity Distribution")
                    sens_df = pd.DataFrame(sens_data)
                    st.bar_chart(sens_df.set_index('sensitivity'))
                
                # Tag cloud
                tag_result = session.run("""
                    MATCH (t:Table)-[:TAGGED_WITH]->(tag:Tag)
                    RETURN tag.name as tag, count(t) as usage_count
                    ORDER BY usage_count DESC
                    LIMIT 10
                """)
                
                tag_data = [record.data() for record in tag_result]
                
                if tag_data:
                    st.subheader("🏷️ Most Used Tags")
                    tag_df = pd.DataFrame(tag_data)
                    st.bar_chart(tag_df.set_index('tag'))
        
        except Exception as e:
            st.error(f"Analytics error: {str(e)}")
    
    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()