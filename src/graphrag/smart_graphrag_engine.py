# src/graphrag/smart_graphrag_engine.py

"""
Smart GraphRAG Engine with Query-Type Routing
UPDATED: Now supports Databricks queries + Cross-source discovery

Routes different question types to optimal retrieval methods:
- semantic_discovery: Hybrid vector + graph (default)
- duplicate_detection: Graph traversal on OLIST_DUPLICATE edges
- relationship_traversal: FK and connection traversal
- metadata_filter: Direct property queries (row count, schema)
- lineage_query: DERIVES_FROM traversal
- databricks_discovery: Databricks table/column queries (NEW)
- cross_source: Cross-platform similarity queries (NEW)
- sensitivity_query: PII/sensitivity classification queries (NEW)
"""

from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import os
import re
import math
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()


class SmartGraphRAGEngine:
    """
    Intelligent GraphRAG with query-type routing.
    Now supports multi-source (Snowflake + Databricks) queries.
    """
    
    def __init__(self):
        """Initialize connections to Milvus, Neo4j, and embedding model."""
        
        # Milvus vector database
        try:
            connections.connect(host='localhost', port='19530')
            self.collection = Collection("table_metadata")
            self.collection.load()
            print("✅ Connected to Milvus")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Milvus: {e}")
        
        # Embedding model
        try:
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ Loaded embedding model (all-MiniLM-L6-v2)")
        except Exception as e:
            raise RuntimeError(f"Failed to load embedding model: {e}")
        
        # Neo4j graph database
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        if not neo4j_password:
            raise ValueError("NEO4J_PASSWORD environment variable not set")
        
        try:
            self.neo4j = GraphDatabase.driver(
                "bolt://localhost:7687",
                auth=("neo4j", neo4j_password)
            )
            with self.neo4j.session() as session:
                session.run("RETURN 1")
            print("✅ Connected to Neo4j")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Neo4j: {e}")
        
        # Configuration
        self.semantic_weight = 0.8
        self.structural_weight = 0.2
        
        print(f"✅ Smart GraphRAG initialized (weights: {self.semantic_weight}/{self.structural_weight})")
    
    def classify_query_type(self, question: str) -> str:
        """
        Classify question into query types.
        """
        q_lower = question.lower()
        
        # PRIORITY 1: Sensitivity queries (most specific)
        sensitivity_keywords = [
            'sensitive', 'sensitivity', 'pii', 'personal',
            'classified', 'confidential', 'private',
            'high sensitivity', 'low sensitivity', 'critical'
        ]
        if any(kw in q_lower for kw in sensitivity_keywords):
            return 'sensitivity_query'
        
        # PRIORITY 2: Cross-source queries
        cross_source_patterns = [
            r'similar.{0,20}(?:across|between|snowflake|databricks)',
            r'(?:snowflake|databricks).{0,20}similar',
            r'cross.{0,10}(?:source|platform|system)',
            r'match.{0,20}(?:between|across)',
            r'databricks.{0,20}(?:like|similar to|match).{0,20}snowflake',
            r'snowflake.{0,20}(?:like|similar to|match).{0,20}databricks',
            r'similar_to',
            r'same data.{0,10}(?:across|in both)',
        ]
        if any(re.search(pattern, q_lower) for pattern in cross_source_patterns):
            return 'cross_source'
        
        # PRIORITY 3: Databricks-specific queries
        databricks_keywords = [
            'databricks', 'unity catalog', 'workspace.sample_data',
            'sales_transactions', 'customer_feedback',
            'federated table', 'federated column'
        ]
        if any(kw in q_lower for kw in databricks_keywords):
            # Sub-classify Databricks queries
            if 'column' in q_lower:
                return 'databricks_discovery'
            if any(w in q_lower for w in ['owner', 'who owns']):
                return 'databricks_discovery'
            return 'databricks_discovery'
        
        # PRIORITY 4: Duplicate detection
        duplicate_keywords = [
            'duplicate', 'duplicates', 'exact cop', 'same as',
            'versions of', 'renamed', 'copies', 'copy of', 'similar table'
        ]
        if any(kw in q_lower for kw in duplicate_keywords):
            return 'duplicate_detection'
        
        # PRIORITY 5: Lineage queries
        lineage_keywords = [
            'derives from', 'derived from', 'derive from',
            'feeds into', 'feed into', 'upstream', 'downstream',
            'lineage', 'source of', 'created from', 'built from',
            'depends on', 'dependency', 'dependencies'
        ]
        if any(kw in q_lower for kw in lineage_keywords):
            return 'lineage_query'
        
        # PRIORITY 6: Relationship queries
        relationship_keywords = [
            'connect to', 'connects to', 'connected to',
            'reference', 'references', 'referenced by',
            'foreign key', 'fk', 'linked to', 'links to',
            'related to', 'relates to'
        ]
        if any(kw in q_lower for kw in relationship_keywords):
            return 'relationship_traversal'
        
        # PRIORITY 7: Metadata filters
        metadata_patterns = [
            r'most rows', r'largest', r'biggest',
            r'smallest', r'fewest rows', r'least rows',
            r'more than \d+', r'greater than \d+',
            r'less than \d+', r'fewer than \d+',
            r'>\s*\d+k?\s*rows', r'<\s*\d+k?\s*rows',
            r'\d+k\+\s*rows',
        ]
        if any(re.search(pattern, q_lower) for pattern in metadata_patterns):
            return 'metadata_filter'
        
        # Schema membership
        if ('tables in' in q_lower or 'tables are in' in q_lower) and 'schema' in q_lower:
            return 'metadata_filter'
        
        # PRIORITY 8: Default to semantic discovery
        return 'semantic_discovery'
    
    def query(self, nl_question: str, top_k: int = 5) -> Dict:
        """Main query method with smart routing."""
        print(f"\n🔍 Question: '{nl_question}'")
        
        query_type = self.classify_query_type(nl_question)
        print(f"🎯 Query type: {query_type}")
        print("=" * 70)
        
        # Route to appropriate handler
        handlers = {
            'sensitivity_query': self._sensitivity_query,
            'cross_source': self._cross_source_query,
            'databricks_discovery': self._databricks_discovery_query,
            'metadata_filter': self._metadata_filter_query,
            'duplicate_detection': self._duplicate_query,
            'relationship_traversal': self._relationship_query,
            'lineage_query': self._lineage_query,
            'semantic_discovery': self._hybrid_search_query
        }
        
        handler = handlers.get(query_type, self._hybrid_search_query)
        results = handler(nl_question, top_k)
        
        print("=" * 70)
        
        return {
            'question': nl_question,
            'query_type': query_type,
            'results': results,
            'total_found': len(results)
        }
    
    # ================================================
    # NEW: DATABRICKS-SPECIFIC HANDLERS
    # ================================================
    
    def _sensitivity_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle sensitivity/PII classification queries."""
        print("🔐 Handler: Sensitivity Query (Databricks columns)")
        
        q_lower = question.lower()
        
        with self.neo4j.session() as session:
            # Check for specific sensitivity level
            if 'high' in q_lower or 'critical' in q_lower:
                sensitivity_filter = "c.sensitivity IN ['High', 'Critical', 'high', 'critical']"
            elif 'low' in q_lower:
                sensitivity_filter = "c.sensitivity IN ['Low', 'low']"
            elif 'medium' in q_lower:
                sensitivity_filter = "c.sensitivity IN ['Medium', 'medium']"
            else:
                # All sensitivities
                sensitivity_filter = "c.sensitivity IS NOT NULL"
            
            result = session.run(f"""
                MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn)
                WHERE t.source = 'databricks' AND {sensitivity_filter}
                RETURN t.full_name as table,
                       t.row_count as rows,
                       c.name as column_name,
                       c.sensitivity as sensitivity,
                       c.data_type as data_type,
                       100.0 as score,
                       0 as centrality,
                       [] as neighbors,
                       'sensitivity: ' + coalesce(c.sensitivity, 'unknown') as reasoning
                ORDER BY c.sensitivity DESC, t.full_name
                LIMIT $top_k
            """, top_k=top_k)
            
            return [dict(record) for record in result]
    
    def _cross_source_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle cross-source similarity queries."""
        print("🔀 Handler: Cross-Source Query (SIMILAR_TO traversal)")
        
        q_lower = question.lower()
        
        # Extract specific table name if mentioned
        table_patterns = {
            'sales_transactions': 'sales_transactions',
            'customer_feedback': 'customer_feedback',
            'customers': 'CUSTOMERS',
            'orders': 'ORDERS',
            'products': 'PRODUCTS',
        }
        
        target_table = None
        for keyword, table_name in table_patterns.items():
            if keyword in q_lower:
                target_table = table_name
                break
        
        with self.neo4j.session() as session:
            if target_table:
                # Specific table cross-source matches
                if target_table in ['sales_transactions', 'customer_feedback']:
                    # Databricks table -> find Snowflake matches
                    result = session.run("""
                        MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                        WHERE db.table_name = $table
                        RETURN sf.schema + '.' + sf.name as table,
                               sf.row_count as rows,
                               sf.schema as schema,
                               r.score * 100 as score,
                               1 as centrality,
                               [db.full_name, 'score: ' + toString(round(r.score * 100, 1)) + '%'] as neighbors,
                               'cross-source match from Databricks ' + db.table_name as reasoning
                        ORDER BY r.score DESC
                        LIMIT $top_k
                    """, table=target_table, top_k=top_k)
                else:
                    # Snowflake table -> find Databricks matches
                    result = session.run("""
                        MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                        WHERE sf.name = $table
                        RETURN db.full_name as table,
                               db.row_count as rows,
                               'databricks' as schema,
                               r.score * 100 as score,
                               1 as centrality,
                               [sf.schema + '.' + sf.name, 'score: ' + toString(round(r.score * 100, 1)) + '%'] as neighbors,
                               'cross-source match to Snowflake ' + sf.name as reasoning
                        ORDER BY r.score DESC
                        LIMIT $top_k
                    """, table=target_table, top_k=top_k)
                
                return [dict(record) for record in result]
            
            # All cross-source matches
            result = session.run("""
                MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                RETURN db.full_name + ' ↔ ' + sf.schema + '.' + sf.name as table,
                       sf.row_count as rows,
                       'cross-source' as schema,
                       r.score * 100 as score,
                       1 as centrality,
                       [r.confidence, 'semantic: ' + toString(round(coalesce(r.semantic_score, 0) * 100, 1)) + '%'] as neighbors,
                       'SIMILAR_TO relationship (score: ' + toString(round(r.score * 100, 1)) + '%)' as reasoning
                ORDER BY r.score DESC
                LIMIT $top_k
            """, top_k=top_k)
            
            return [dict(record) for record in result]
    
    def _databricks_discovery_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle Databricks-specific metadata discovery."""
        print("🧱 Handler: Databricks Discovery")
        
        q_lower = question.lower()
        
        with self.neo4j.session() as session:
            # Column-specific queries
            if 'column' in q_lower:
                # Check for specific table
                if 'sales_transactions' in q_lower:
                    table_filter = "t.table_name = 'sales_transactions'"
                elif 'customer_feedback' in q_lower:
                    table_filter = "t.table_name = 'customer_feedback'"
                else:
                    table_filter = "1=1"  # All tables
                
                result = session.run(f"""
                    MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn)
                    WHERE t.source = 'databricks' AND {table_filter}
                    RETURN t.full_name + '.' + c.name as table,
                           t.row_count as rows,
                           c.data_type as schema,
                           100.0 as score,
                           c.position as centrality,
                           [c.sensitivity, c.nullable] as neighbors,
                           'column: ' + c.name + ' (' + replace(c.data_type, 'ColumnTypeName.', '') + ')' as reasoning
                    ORDER BY t.full_name, c.position
                    LIMIT $top_k
                """, top_k=top_k)
                
                return [dict(record) for record in result]
            
            # Owner queries
            if 'owner' in q_lower or 'who owns' in q_lower:
                result = session.run("""
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks'
                    RETURN t.full_name as table,
                           t.row_count as rows,
                           t.owner as schema,
                           100.0 as score,
                           t.column_count as centrality,
                           [t.owner] as neighbors,
                           'owner: ' + coalesce(t.owner, 'unknown') as reasoning
                    ORDER BY t.owner, t.full_name
                    LIMIT $top_k
                """, top_k=top_k)
                
                return [dict(record) for record in result]
            
            # Default: list all Databricks tables
            result = session.run("""
                MATCH (t:FederatedTable)
                WHERE t.source = 'databricks'
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:FederatedColumn)
                WITH t, count(c) as col_count
                RETURN t.full_name as table,
                       t.row_count as rows,
                       t.owner as schema,
                       100.0 as score,
                       col_count as centrality,
                       [t.owner, toString(col_count) + ' columns'] as neighbors,
                       'Databricks table' as reasoning
                ORDER BY t.full_name
                LIMIT $top_k
            """, top_k=top_k)
            
            return [dict(record) for record in result]
    
    # ================================================
    # EXISTING HANDLERS
    # ================================================
    
    def _metadata_filter_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle metadata filtering queries."""
        print("📊 Handler: Metadata Filter")
        q_lower = question.lower()
        
        with self.neo4j.session() as session:
            # Row threshold parsing
            threshold_info = self._parse_row_threshold(question)
            if threshold_info:
                threshold, operator = threshold_info
                op_symbol = '>=' if operator == 'gte' else '>' if operator == 'gt' else '<=' if operator == 'lte' else '<'
                
                result = session.run(f"""
                    MATCH (t:OlistData)
                    WHERE t.row_count {op_symbol} $threshold
                    RETURN t.schema + '.' + t.name as table,
                           t.row_count as rows,
                           t.schema as schema,
                           100.0 as score,
                           0 as centrality,
                           [] as neighbors,
                           'row_count {op_symbol} ' + toString($threshold) as reasoning
                    ORDER BY t.row_count DESC
                """, threshold=threshold)
                return [dict(record) for record in result]
            
            # Largest tables
            if any(kw in q_lower for kw in ['most rows', 'largest', 'biggest']):
                result = session.run("""
                    MATCH (t:OlistData)
                    RETURN t.schema + '.' + t.name as table,
                           t.row_count as rows,
                           t.schema as schema,
                           100.0 as score,
                           0 as centrality,
                           [] as neighbors,
                           'sorted by row_count DESC' as reasoning
                    ORDER BY t.row_count DESC
                    LIMIT $top_k
                """, top_k=top_k)
                return [dict(record) for record in result]
            
            return []
    
    def _parse_row_threshold(self, question: str) -> Optional[tuple]:
        """Parse row count threshold from question."""
        q_lower = question.lower()
        
        patterns = [
            (r'(>=?)\s*(\d+)k\s*rows?', lambda m: (int(m.group(2)) * 1000, 'gte' if m.group(1) == '>=' else 'gt')),
            (r'(<=?)\s*(\d+)k\s*rows?', lambda m: (int(m.group(2)) * 1000, 'lte' if m.group(1) == '<=' else 'lt')),
            (r'(\d+)k\+\s*rows?', lambda m: (int(m.group(1)) * 1000, 'gt')),
            (r'more than (\d+)', lambda m: (int(m.group(1)), 'gt')),
            (r'less than (\d+)', lambda m: (int(m.group(1)), 'lt')),
        ]
        
        for pattern, extractor in patterns:
            match = re.search(pattern, q_lower)
            if match:
                return extractor(match)
        
        return None
    
    def _duplicate_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle duplicate detection queries."""
        print("🔄 Handler: Duplicate Detection")
        
        with self.neo4j.session() as session:
            result = session.run("""
                MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData)
                RETURN t2.schema + '.' + t2.name as table,
                       t2.row_count as rows,
                       t2.schema as schema,
                       d.confidence * 100 as score,
                       0 as centrality,
                       [t1.schema + '.' + t1.name] as neighbors,
                       'duplicate of ' + t1.name as reasoning
                LIMIT $top_k
            """, top_k=top_k)
            return [dict(record) for record in result]
    
    def _relationship_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle relationship traversal queries."""
        print("🔗 Handler: Relationship Traversal")
        # Placeholder for deeper relationship traversal logic if needed
        return []
    
    def _lineage_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle lineage queries."""
        print("📊 Handler: Lineage Query")
        
        with self.neo4j.session() as session:
            result = session.run("""
                MATCH (target:OlistData)-[r:DERIVES_FROM]->(source:OlistData)
                RETURN target.schema + '.' + target.name as table,
                       target.row_count as rows,
                       target.schema as schema,
                       r.confidence * 100 as score,
                       1 as centrality,
                       [source.schema + '.' + source.name, r.lineage_type] as neighbors,
                       target.name + ' derives from ' + source.name as reasoning
                ORDER BY r.confidence DESC
                LIMIT $top_k
            """, top_k=top_k)
            return [dict(record) for record in result]
    
    def _hybrid_search_query(self, question: str, top_k: int) -> List[Dict]:
        """Handle semantic discovery with hybrid ranking."""
        print("🔍 Handler: Hybrid GraphRAG (Semantic + Structural)")
        
        # Semantic search via Milvus
        semantic_results = self._semantic_search(question, top_k=10)
        
        # Enrich with graph context
        graph_context = self._get_graph_context(semantic_results)
        
        # Hybrid ranking
        ranked_results = self._hybrid_rank(semantic_results, graph_context)
        
        return ranked_results[:top_k]
    
    def _semantic_search(self, question: str, top_k: int = 10, source_filter: str = None) -> List:
        """Vector similarity search in Milvus."""
        query_embedding = self.model.encode(question).tolist()
        
        # Build filter expression for source
        expr = None
        if source_filter:
            expr = f'source == "{source_filter}"'
        
        results = self.collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k,
            output_fields=["text", "source", "table_name"],
            expr=expr
        )
        
        return results[0]
    
    def _get_graph_context(self, semantic_results) -> Dict:
        """Get graph context for semantic results."""
        table_names = []
        sources = []
        
        for hit in semantic_results:
            # FIX applied here: Remove second argument to .get()
            text = hit.entity.get('text')
            source = hit.entity.get('source')
            table_name = hit.entity.get('table_name')
            
            # Handle defaults manually
            if text is None: text = ''
            if source is None: source = 'snowflake'
            if table_name is None: table_name = ''
            
            if not table_name:
                # Fallback: parse from text
                table_part = text.split(' (')[0]
                table_name = table_part.split('.')[-1]
            
            table_names.append(table_name)
            sources.append(source)
        
        context = {}
        
        with self.neo4j.session() as session:
            # Snowflake tables
            sf_tables = [t for t, s in zip(table_names, sources) if s == 'snowflake']
            if sf_tables:
                result = session.run("""
                    MATCH (t:OlistData)
                    WHERE t.name IN $table_names
                    OPTIONAL MATCH (t)-[r]-(related:OlistData)
                    WHERE type(r) <> 'OLIST_DUPLICATE'
                    WITH t, count(DISTINCT r) as centrality, collect(DISTINCT related.name)[..3] as neighbors
                    RETURN t.name as table, t.schema as schema, t.row_count as rows, centrality, neighbors, 'snowflake' as source
                """, table_names=sf_tables)
                for record in result:
                    context[record['table']] = dict(record)
            
            # Databricks tables
            db_tables = [t for t, s in zip(table_names, sources) if s == 'databricks']
            if db_tables:
                result = session.run("""
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks' AND t.table_name IN $table_names
                    OPTIONAL MATCH (t)-[r:SIMILAR_TO]-(related)
                    WITH t, count(DISTINCT r) as centrality, collect(DISTINCT related.name)[..3] as neighbors
                    RETURN t.table_name as table, 'databricks' as schema, t.row_count as rows, centrality, neighbors, 'databricks' as source
                """, table_names=db_tables)
                for record in result:
                    context[record['table']] = dict(record)
        
        return context
    
    def _hybrid_rank(self, semantic_results, graph_context: Dict) -> List[Dict]:
        """Hybrid ranking with source awareness."""
        ranked = []
        max_centrality = 6
        
        for hit in semantic_results:
            # FIX applied here: Remove second argument to .get()
            text = hit.entity.get('text')
            source = hit.entity.get('source')
            table_name = hit.entity.get('table_name')
            
            # Handle defaults manually
            if text is None: text = ''
            if source is None: source = 'snowflake'
            if table_name is None: table_name = ''
            
            if not table_name:
                table_part = text.split(' (')[0]
                table_name = table_part.split('.')[-1]
            
            # Get full table identifier
            if source == 'databricks':
                full_name = f"databricks.{table_name}"
            else:
                schema = text.split('.')[0] if '.' in text else 'UNKNOWN'
                full_name = f"{schema}.{table_name}"
            
            semantic_score = hit.distance
            
            graph_data = graph_context.get(table_name, {})
            centrality = graph_data.get('centrality', 0)
            
            if centrality > 0:
                structural_score = math.log(centrality + 1) / math.log(max_centrality + 1)
            else:
                structural_score = 0.0
            
            final_score = (self.semantic_weight * semantic_score) + \
                         (self.structural_weight * structural_score)
            
            # Source indicator
            source_icon = "🧱" if source == 'databricks' else "❄️"
            
            ranked.append({
                'table': full_name,
                'rows': graph_data.get('rows', 0),
                'schema': graph_data.get('schema', source),
                'source': source,
                'semantic_score': round(semantic_score * 100, 1),
                'structural_score': round(structural_score * 100, 1),
                'score': round(final_score * 100, 1),
                'centrality': centrality,
                'neighbors': graph_data.get('neighbors', []),
                'reasoning': f"{source_icon} {source}: semantic={semantic_score:.2f}, structural={structural_score:.2f}"
            })
        
        return sorted(ranked, key=lambda x: x['score'], reverse=True)
    
    def close(self):
        """Clean up database connections."""
        try:
            self.neo4j.close()
            print("✅ Neo4j connection closed")
        except:
            pass
        
        try:
            connections.disconnect("default")
            print("✅ Milvus connection closed")
        except:
            pass


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    engine = SmartGraphRAGEngine()
    
    print("\n" + "🎯" * 35)
    print("SMART ROUTING TEST (with Databricks)")
    print("🎯" * 35)
    
    tests = [
        # Existing tests
        ("Which tables contain customer data?", "semantic_discovery"),
        ("Show me all duplicate tables", "duplicate_detection"),
        # Databricks tests
        ("Show me Databricks tables", "databricks_discovery"),
        ("What columns are in sales_transactions?", "databricks_discovery"),
        ("Which columns have high sensitivity?", "sensitivity_query"),
        ("Find cross-source matches for CUSTOMERS", "cross_source"),
        ("What Databricks tables are similar to Snowflake?", "cross_source"),
    ]
    
    correct = 0
    for question, expected_type in tests:
        actual_type = engine.classify_query_type(question)
        status = "✅" if actual_type == expected_type else "❌"
        print(f"{status} '{question[:45]}...' → {actual_type}")
        if actual_type == expected_type:
            correct += 1
    
    print(f"\n🎯 Classification Accuracy: {correct}/{len(tests)} = {correct/len(tests)*100:.0f}%")
    
    engine.close()