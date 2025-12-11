# src/graphrag/explainable_graphrag.py

"""
Explainable GraphRAG - Natural Language Explanations for Cross-Source Matching

NEW FEATURE: Explains WHY tables are similar using SANTOS score breakdown.

Author: Pranav Kharat
Date: December 2025
"""

from .llm_enhanced_smart_graphrag import LLMEnhancedSmartGraphRAG
from neo4j import GraphDatabase
from langchain_ollama import ChatOllama
from typing import Dict, List, Optional
import os


class ExplainableGraphRAG(LLMEnhancedSmartGraphRAG):
    """
    Extends LLM-Enhanced Smart GraphRAG with rich explanations for:
    - Cross-source similarity (WHY tables match across Snowflake/Databricks)
    - Databricks metadata discovery
    - Sensitivity classifications
    - Duplicate detection reasoning
    """
    
    def __init__(self, temperature: float = 0.3):
        super().__init__(temperature=temperature)
        print("✅ Explainable GraphRAG initialized (with cross-source explanations)")
    
    def query(self, nl_question: str, top_k: int = 5) -> Dict:
        """Enhanced query with rich explanations for all query types."""
        result = super().query(nl_question, top_k)
        query_type = result.get('query_type', 'semantic_discovery')
        
        if query_type == 'cross_source' and result.get('results'):
            result['nl_answer'] = self._explain_cross_source_match(
                nl_question, result['results']
            )
            result['explanation_type'] = 'cross_source_detailed'
            
        elif query_type == 'databricks_discovery' and result.get('results'):
            result['nl_answer'] = self._explain_databricks_discovery(
                nl_question, result['results']
            )
            result['explanation_type'] = 'databricks_detailed'
            
        elif query_type == 'sensitivity_query' and result.get('results'):
            result['nl_answer'] = self._explain_sensitivity(
                nl_question, result['results']
            )
            result['explanation_type'] = 'sensitivity_detailed'
        
        return result
    
    def _explain_cross_source_match(self, question: str, results: List[Dict]) -> str:
        """Generate detailed explanation for cross-source matches."""
        if not results:
            return "No cross-source matches found."
        
        match_details = self._get_cross_source_details(results[:3])
        
        if not match_details:
            return self._template_answer(results)
        
        context = self._build_cross_source_context(match_details)
        
        prompt = f"""You are a data catalog expert explaining WHY tables from different platforms are similar.

QUESTION: {question}

CROSS-SOURCE MATCHES (Snowflake ↔ Databricks):
{context}

SCORING METHODOLOGY (SANTOS Algorithm):
- Semantic Score: Column name similarity using embeddings (40% weight)
- Schema Score: Data type and structure overlap (25% weight)  
- Statistical Score: Row count and column count similarity (20% weight)
- Relationship Score: Foreign key pattern matching (15% weight)

INSTRUCTIONS:
1. Start with the top match and explain WHY it's similar
2. Reference specific score components (e.g., "high semantic similarity due to matching columns...")
3. Mention the matching columns if available
4. Explain what this means for data governance (potential duplicates, federation opportunities)
5. Keep response to 3-4 sentences, conversational tone
6. Do NOT just list numbers - interpret what they mean

EXPLANATION:"""
        
        try:
            response = self.llm.invoke(prompt)
            answer = response.content.strip()
            
            if len(answer) < 30 or "i don't know" in answer.lower():
                return self._template_cross_source_answer(match_details)
            
            return answer
            
        except Exception as e:
            print(f"⚠️ LLM explanation failed: {e}")
            return self._template_cross_source_answer(match_details)
    
    def _get_cross_source_details(self, results: List[Dict]) -> List[Dict]:
        """Fetch detailed SIMILAR_TO relationship properties from Neo4j."""
        details = []
        
        try:
            with self.neo4j.session() as session:
                result = session.run("""
                    MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                    WHERE db.source = 'databricks'
                    RETURN db.table_name as databricks_table,
                           db.full_name as databricks_full_name,
                           db.row_count as db_rows,
                           db.owner as db_owner,
                           sf.schema + '.' + sf.name as snowflake_table,
                           sf.row_count as sf_rows,
                           sf.owner as sf_owner,
                           coalesce(r.score, 0) as total_score,
                           coalesce(r.confidence, 'unknown') as confidence,
                           coalesce(r.semantic_score, r.column_semantic_score, 0) as semantic_score,
                           coalesce(r.schema_score, r.type_overlap_score, 0) as schema_score,
                           coalesce(r.statistical_score, 0) as statistical_score,
                           coalesce(r.relationship_score, 0) as relationship_score,
                           r.matching_columns as matching_columns
                    ORDER BY r.score DESC
                    LIMIT 5
                """)
                
                for record in result:
                    details.append(dict(record))
        except Exception as e:
            print(f"⚠️ Error fetching cross-source details: {e}")
        
        return details
    
    def _build_cross_source_context(self, match_details: List[Dict]) -> str:
        """Build rich context from SIMILAR_TO relationship properties."""
        context_lines = []
        
        for i, match in enumerate(match_details, 1):
            total_score = float(match.get('total_score') or 0)
            confidence = match.get('confidence') or 'unknown'
            
            lines = [
                f"{i}. **{match.get('databricks_table', 'Unknown')}** (Databricks) ↔ **{match.get('snowflake_table', 'Unknown')}** (Snowflake)",
                f"   Overall Score: {total_score:.1%} ({confidence} confidence)",
                f"   ",
                f"   SANTOS Score Breakdown:",
            ]
            
            semantic = match.get('semantic_score')
            if semantic is not None:
                lines.append(f"   • Semantic (column names): {float(semantic):.1%}")
            schema = match.get('schema_score')
            if schema is not None:
                lines.append(f"   • Schema (types/structure): {float(schema):.1%}")
            statistical = match.get('statistical_score')
            if statistical is not None:
                lines.append(f"   • Statistical (row/col counts): {float(statistical):.1%}")
            relationship = match.get('relationship_score')
            if relationship is not None:
                lines.append(f"   • Relationship (FK patterns): {float(relationship):.1%}")
            
            if match.get('matching_columns'):
                cols = match['matching_columns']
                if isinstance(cols, list) and len(cols) > 0:
                    col_str = ', '.join([f"{c[0]}↔{c[1]}" if isinstance(c, (list, tuple)) else str(c) for c in cols[:3]])
                    lines.append(f"   Matching Columns: {col_str}")
            
            lines.append(f"   ")
            lines.append(f"   Databricks: {match.get('db_rows') or 0:,} rows, owner: {match.get('db_owner') or 'unknown'}")
            lines.append(f"   Snowflake: {match.get('sf_rows') or 0:,} rows, owner: {match.get('sf_owner') or 'unknown'}")
            
            context_lines.append("\n".join(lines))
        
        return "\n\n".join(context_lines)
    
    def _template_cross_source_answer(self, match_details: List[Dict]) -> str:
        """Fallback template for cross-source explanations."""
        if not match_details:
            return "No cross-source matches found between Databricks and Snowflake."
        
        top = match_details[0]
        
        total_score = float(top.get('total_score') or 0)
        semantic_score = float(top.get('semantic_score') or 0)
        schema_score = float(top.get('schema_score') or 0)
        statistical_score = float(top.get('statistical_score') or 0)
        relationship_score = float(top.get('relationship_score') or 0)
        
        explanations = []
        
        if semantic_score > 0.3:
            explanations.append("strong column name similarity")
        elif semantic_score > 0.15:
            explanations.append("moderate column name overlap")
        
        if schema_score > 0.3:
            explanations.append("matching data type patterns")
        
        if statistical_score > 0.3:
            explanations.append("similar table sizes")
        
        if relationship_score > 0.2:
            explanations.append("common foreign key patterns")
        
        reason = ", ".join(explanations) if explanations else "shared semantic characteristics"
        
        db_table = top.get('databricks_table') or top.get('databricks_full_name') or 'Unknown'
        sf_table = top.get('snowflake_table') or 'Unknown'
        
        answer = (
            f"The Databricks table **{db_table}** is most similar to "
            f"Snowflake's **{sf_table}** with a {total_score:.1%} match score. "
            f"This similarity is driven by {reason}. "
        )
        
        if top.get('matching_columns'):
            cols = top['matching_columns']
            if isinstance(cols, list) and len(cols) > 0:
                col_examples = []
                for c in cols[:2]:
                    if isinstance(c, (list, tuple)) and len(c) >= 2:
                        col_examples.append(f"{c[0]}↔{c[1]}")
                    else:
                        col_examples.append(str(c))
                if col_examples:
                    answer += f"Key matching columns include: {', '.join(col_examples)}. "
        
        if total_score > 0.35:
            answer += "This high similarity suggests these may be duplicates or derived tables worth consolidating."
        elif total_score > 0.25:
            answer += "Consider reviewing these tables for potential federation or data lineage relationships."
        
        return answer
    
    def _explain_databricks_discovery(self, question: str, results: List[Dict]) -> str:
        """Generate explanation for Databricks metadata queries."""
        if not results:
            return "No Databricks tables found matching your query."
        
        db_details = self._get_databricks_details(results[:3])
        
        if not db_details:
            return self._template_answer(results)
        
        context = self._build_databricks_context(db_details)
        
        prompt = f"""You are a data catalog assistant explaining Databricks table metadata.

QUESTION: {question}

DATABRICKS TABLES:
{context}

INSTRUCTIONS:
1. Directly answer the question using the table information above
2. Mention sensitivity classifications if relevant to the query
3. Note the owner for data stewardship context
4. Keep response to 2-3 sentences
5. Be specific - use actual table and column names

RESPONSE:"""
        
        try:
            response = self.llm.invoke(prompt)
            return response.content.strip()
        except:
            return self._template_databricks_answer(db_details)
    
    def _get_databricks_details(self, results: List[Dict]) -> List[Dict]:
        """Fetch Databricks table details including columns and sensitivity."""
        details = []
        
        try:
            with self.neo4j.session() as session:
                result = session.run("""
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks'
                    OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:FederatedColumn)
                    WITH t, collect({
                        name: c.name, 
                        type: c.data_type, 
                        sensitivity: c.sensitivity
                    }) as columns
                    RETURN t.table_name as name,
                           t.full_name as full_name,
                           t.row_count as row_count,
                           t.owner as owner,
                           columns
                    ORDER BY t.row_count DESC
                    LIMIT 5
                """)
                
                for record in result:
                    details.append(dict(record))
        except Exception as e:
            print(f"⚠️ Error fetching Databricks details: {e}")
        
        return details
    
    def _build_databricks_context(self, db_details: List[Dict]) -> str:
        """Build context for Databricks explanations."""
        lines = []
        
        for i, table in enumerate(db_details, 1):
            row_count = table.get('row_count') or 0
            lines.append(f"{i}. **{table.get('full_name', 'Unknown')}** ({row_count:,} rows)")
            lines.append(f"   Owner: {table.get('owner') or 'unknown'}")
            
            if table.get('columns'):
                cols = table['columns']
                high_sens = [c['name'] for c in cols if c.get('sensitivity') in ['High', 'Critical']]
                med_sens = [c['name'] for c in cols if c.get('sensitivity') == 'Medium']
                
                if high_sens:
                    lines.append(f"   ⚠️ High/Critical sensitivity: {', '.join(high_sens[:3])}")
                if med_sens:
                    lines.append(f"   📋 Medium sensitivity: {', '.join(med_sens[:3])}")
                
                col_names = [c['name'] for c in cols[:5] if c.get('name')]
                if col_names:
                    lines.append(f"   Columns: {', '.join(col_names)}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _template_databricks_answer(self, db_details: List[Dict]) -> str:
        """Fallback template for Databricks queries."""
        if not db_details:
            return "No Databricks tables found."
        
        top = db_details[0]
        row_count = top.get('row_count') or 0
        answer = f"Found **{top.get('full_name', 'Unknown')}** with {row_count:,} rows, owned by {top.get('owner') or 'unknown'}. "
        
        if top.get('columns'):
            cols = top['columns']
            high_sens = [c['name'] for c in cols if c.get('sensitivity') in ['High', 'Critical']]
            if high_sens:
                answer += f"Contains sensitive columns: {', '.join(high_sens[:2])}. "
        
        return answer
    
    def _explain_sensitivity(self, question: str, results: List[Dict]) -> str:
        """Generate explanation for sensitivity-related queries."""
        sens_details = self._get_sensitivity_details()
        
        prompt = f"""You are a data governance expert explaining data sensitivity classifications.

QUESTION: {question}

SENSITIVITY BREAKDOWN:
{sens_details}

INSTRUCTIONS:
1. Answer the specific question about sensitivity
2. Explain what the sensitivity levels mean for data handling
3. Recommend appropriate access controls if relevant
4. Keep response to 2-3 sentences

RESPONSE:"""
        
        try:
            response = self.llm.invoke(prompt)
            return response.content.strip()
        except:
            return f"Found columns with varying sensitivity levels. High/Critical columns require restricted access."
    
    def _get_sensitivity_details(self) -> str:
        """Get sensitivity classification summary."""
        try:
            with self.neo4j.session() as session:
                result = session.run("""
                    MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn)
                    WHERE t.source = 'databricks' AND c.sensitivity IS NOT NULL
                    RETURN c.sensitivity as level, 
                           collect(DISTINCT t.table_name + '.' + c.name)[..5] as columns,
                           count(*) as count
                    ORDER BY 
                        CASE c.sensitivity 
                            WHEN 'Critical' THEN 1 
                            WHEN 'High' THEN 2 
                            WHEN 'Medium' THEN 3 
                            ELSE 4 
                        END
                """)
                
                lines = []
                for record in result:
                    level = record['level']
                    count = record['count']
                    examples = record['columns'][:3]
                    lines.append(f"• {level}: {count} columns (e.g., {', '.join(examples)})")
                
                return "\n".join(lines) if lines else "No sensitivity classifications found."
        except Exception as e:
            return f"Error retrieving sensitivity data: {e}"
    
    def explain_match(self, databricks_table: str, snowflake_table: str) -> str:
        """
        Dedicated method to explain a specific cross-source match.
        
        Args:
            databricks_table: Databricks table name (e.g., 'sales_transactions')
            snowflake_table: Snowflake table name (e.g., 'ORDERS' or 'OLIST_SALES.ORDERS')
        """
        databricks_table = (databricks_table or "").strip()
        snowflake_table = (snowflake_table or "").strip().upper()
        
        if not databricks_table or not snowflake_table:
            return "Please provide both table names."
        
        try:
            with self.neo4j.session() as session:
                # First try: exact match
                result = session.run("""
                    MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                    WHERE toLower(db.table_name) = toLower($db_table) 
                      AND (toUpper(sf.name) = $sf_table OR toUpper(sf.schema + '.' + sf.name) = $sf_table)
                    OPTIONAL MATCH (db)-[:HAS_COLUMN]->(dbc:FederatedColumn)
                    OPTIONAL MATCH (sf)-[:HAS_COLUMN]->(sfc:OlistColumn)
                    RETURN db.table_name as db_name,
                           db.full_name as db_full_name,
                           sf.schema + '.' + sf.name as sf_name,
                           db.row_count as db_rows,
                           sf.row_count as sf_rows,
                           db.owner as db_owner,
                           sf.owner as sf_owner,
                           r.score as score,
                           r.confidence as confidence,
                           coalesce(r.semantic_score, r.column_semantic_score, 0) as semantic,
                           coalesce(r.schema_score, r.type_overlap_score, 0) as schema,
                           coalesce(r.statistical_score, 0) as statistical,
                           coalesce(r.relationship_score, 0) as relationship,
                           r.matching_columns as matching_cols,
                           collect(DISTINCT dbc.name)[..10] as db_columns,
                           collect(DISTINCT sfc.name)[..10] as sf_columns
                    LIMIT 1
                """, db_table=databricks_table, sf_table=snowflake_table)
                
                record = result.single()
                
                # Second try: without schema prefix
                if not record:
                    sf_name_only = snowflake_table.split('.')[-1]
                    result = session.run("""
                        MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                        WHERE toLower(db.table_name) = toLower($db_table) 
                          AND toUpper(sf.name) = $sf_table
                        RETURN db.table_name as db_name,
                               sf.schema + '.' + sf.name as sf_name,
                               db.row_count as db_rows,
                               sf.row_count as sf_rows,
                               coalesce(r.score, 0) as score,
                               r.confidence as confidence,
                               coalesce(r.semantic_score, 0) as semantic,
                               coalesce(r.schema_score, 0) as schema,
                               coalesce(r.statistical_score, 0) as statistical,
                               coalesce(r.relationship_score, 0) as relationship,
                               [] as db_columns,
                               [] as sf_columns
                        LIMIT 1
                    """, db_table=databricks_table, sf_table=sf_name_only)
                    record = result.single()
                
                if not record:
                    # Show available matches for this Databricks table
                    avail = session.run("""
                        MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                        WHERE toLower(db.table_name) = toLower($db_table)
                        RETURN sf.name as snowflake_table, r.score as score
                        ORDER BY r.score DESC LIMIT 3
                    """, db_table=databricks_table)
                    
                    matches = [f"{r['snowflake_table']} ({float(r['score'] or 0):.1%})" for r in avail]
                    if matches:
                        return f"No match found for {databricks_table} ↔ {snowflake_table}. Available matches for {databricks_table}: {', '.join(matches)}"
                    return f"No SIMILAR_TO relationship found between {databricks_table} and {snowflake_table}."
                
                match = dict(record)
            
            # Safe extraction
            score = float(match.get('score') or 0)
            semantic = float(match.get('semantic') or 0)
            schema = float(match.get('schema') or 0)
            statistical = float(match.get('statistical') or 0)
            relationship = float(match.get('relationship') or 0)
            db_rows = int(match.get('db_rows') or 0)
            sf_rows = int(match.get('sf_rows') or 0)
            db_columns = match.get('db_columns') or []
            sf_columns = match.get('sf_columns') or []
            
            prompt = f"""Explain why these two tables are similar in plain English:

DATABRICKS: {match.get('db_name', 'Unknown')} ({db_rows:,} rows)
Columns: {', '.join(db_columns[:5]) if db_columns else 'N/A'}

SNOWFLAKE: {match.get('sf_name', 'Unknown')} ({sf_rows:,} rows)  
Columns: {', '.join(sf_columns[:5]) if sf_columns else 'N/A'}

SIMILARITY SCORES:
- Overall: {score:.1%}
- Semantic (column names): {semantic:.1%}
- Schema (data types): {schema:.1%}
- Statistical (size): {statistical:.1%}
- Relationship (FK patterns): {relationship:.1%}

Write a 3-4 sentence explanation of WHY these tables are similar, what the scores mean, 
and whether they might be duplicates or just related tables. Be specific and actionable."""
            
            try:
                response = self.llm.invoke(prompt)
                return response.content.strip()
            except Exception as e:
                # Template fallback
                strength = 'strong' if semantic > 0.3 else 'moderate' if semantic > 0.15 else 'some'
                action = 'These tables may be duplicates worth consolidating.' if score > 0.35 else 'Consider reviewing for potential data lineage relationships.'
                return (
                    f"**{match.get('db_name', 'Unknown')}** and **{match.get('sf_name', 'Unknown')}** have a {score:.1%} similarity score. "
                    f"The semantic similarity is {semantic:.1%}, indicating {strength} column name overlap. "
                    f"{action}"
                )
                
        except Exception as e:
            return f"Error explaining match: {str(e)}"


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🧪 TESTING EXPLAINABLE GRAPHRAG")
    print("="*70)
    
    engine = ExplainableGraphRAG(temperature=0.3)
    
    # Test explain_match
    print("\nTesting explain_match():")
    print("-"*70)
    
    # First check what matches exist
    with engine.neo4j.session() as session:
        result = session.run("""
            MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
            RETURN db.table_name, sf.name, r.score
            ORDER BY r.score DESC LIMIT 5
        """)
        print("Available matches:")
        for record in result:
            print(f"  {record['db.table_name']} ↔ {record['sf.name']} ({float(record['r.score'] or 0):.1%})")
    
    print("\n" + "="*70)
    print("✅ Testing Complete!")
    print("="*70)
    
    engine.close()