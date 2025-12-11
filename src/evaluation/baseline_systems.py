"""
Baseline Systems for GraphRAG Evaluation
Implements 3 baselines to compare against Smart GraphRAG
"""

from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import os
import re
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# BASELINE 1: KEYWORD SEARCH
# ============================================================================

class KeywordSearchBaseline:
    """
    Simple keyword matching on table names
    No embeddings, no graph - just string matching
    """
    
    def __init__(self):
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        self.neo4j = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        print("✅ Keyword Search Baseline initialized")
    
    def query(self, nl_question: str, top_k: int = 5):
        """Extract keywords and match against table names"""
        
        # Extract keywords (simple approach)
        keywords = self._extract_keywords(nl_question)
        
        with self.neo4j.session() as session:
            # Search for tables containing any keyword
            result = session.run("""
                MATCH (t:OlistData)
                WHERE any(keyword IN $keywords WHERE toLower(t.name) CONTAINS toLower(keyword))
                   OR any(keyword IN $keywords WHERE toLower(t.schema) CONTAINS toLower(keyword))
                WITH t, 
                     reduce(score = 0, keyword IN $keywords | 
                         score + CASE WHEN toLower(t.name) CONTAINS toLower(keyword) THEN 2
                                      WHEN toLower(t.schema) CONTAINS toLower(keyword) THEN 1
                                      ELSE 0 END) as keyword_score
                WHERE keyword_score > 0
                RETURN t.schema + '.' + t.name as table,
                       t.row_count as rows,
                       keyword_score * 10.0 as semantic_score,
                       0.0 as structural_score,
                       keyword_score * 10.0 as final_score,
                       0 as centrality,
                       [] as neighbors,
                       'keyword_match' as reasoning
                ORDER BY keyword_score DESC, t.row_count DESC
                LIMIT $top_k
            """, keywords=keywords, top_k=top_k)
            
            results = [dict(record) for record in result]
        
        return {
            'question': nl_question,
            'query_type': 'keyword_search',
            'results': results,
            'total_found': len(results)
        }
    
    def _extract_keywords(self, question):
        """Extract meaningful keywords from question"""
        # Remove common stop words
        stop_words = {'which', 'what', 'where', 'is', 'are', 'the', 'a', 'an', 'in', 'on', 
                     'find', 'show', 'me', 'all', 'tables', 'table', 'data', 'information'}
        
        # Tokenize and filter
        words = re.findall(r'\b\w+\b', question.lower())
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        
        return keywords
    
    def close(self):
        self.neo4j.close()


# ============================================================================
# BASELINE 2: EMBEDDINGS-ONLY RAG
# ============================================================================

class EmbeddingsOnlyBaseline:
    """
    Pure semantic search using Milvus
    No graph context - only vector similarity
    """
    
    def __init__(self):
        connections.connect(host='localhost', port='19530')
        self.collection = Collection("table_metadata")
        self.collection.load()
        
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✅ Embeddings-Only Baseline initialized")
    
    def query(self, nl_question: str, top_k: int = 5):
        """Pure vector similarity search"""
        
        # Generate query embedding
        query_embedding = self.model.encode(nl_question).tolist()
        
        # Search in Milvus
        results = self.collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k,
            output_fields=["text"]
        )
        
        # Format results
        formatted_results = []
        for hit in results[0]:
            text = hit.entity.get('text')
            table_part = text.split(' (')[0]
            rows_text = text.split(' (')[1].replace(' rows)', '') if '(' in text else '0'
            
            formatted_results.append({
                'table': table_part,
                'rows': int(rows_text),
                'semantic_score': round(hit.distance * 100, 1),
                'structural_score': 0.0,  # No graph context
                'final_score': round(hit.distance * 100, 1),
                'centrality': 0,
                'neighbors': [],
                'reasoning': f'embeddings_only: similarity ({hit.distance:.3f})'
            })
        
        return {
            'question': nl_question,
            'query_type': 'embeddings_only',
            'results': formatted_results,
            'total_found': len(formatted_results)
        }
    
    def close(self):
        pass  # No resources to close


# ============================================================================
# BASELINE 3: GRAPH-ONLY
# ============================================================================

class GraphOnlyBaseline:
    """
    Pure graph traversal using Neo4j
    No semantic embeddings - only Cypher pattern matching
    """
    
    def __init__(self):
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        self.neo4j = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        print("✅ Graph-Only Baseline initialized")
    
    def query(self, nl_question: str, top_k: int = 5):
        """Graph traversal with keyword matching"""
        
        # Extract keywords
        keywords = self._extract_keywords(nl_question)
        
        if not keywords:
            return {'question': nl_question, 'query_type': 'graph_only', 'results': [], 'total_found': 0}
        
        with self.neo4j.session() as session:
            # Use graph centrality as primary ranking
            result = session.run("""
                MATCH (t:OlistData)
                WHERE any(keyword IN $keywords WHERE toLower(t.name) CONTAINS toLower(keyword))
                OPTIONAL MATCH (t)-[r]-(related:OlistData)
                WITH t, count(DISTINCT r) as centrality, collect(DISTINCT related.name)[..3] as neighbors
                RETURN t.schema + '.' + t.name as table,
                       t.row_count as rows,
                       0.0 as semantic_score,
                       centrality * 10.0 as structural_score,
                       centrality * 10.0 as final_score,
                       centrality,
                       neighbors,
                       'graph_only: centrality (' + toString(centrality) + ')' as reasoning
                ORDER BY centrality DESC, t.row_count DESC
                LIMIT $top_k
            """, keywords=keywords, top_k=top_k)
            
            results = [dict(record) for record in result]
        
        return {
            'question': nl_question,
            'query_type': 'graph_only',
            'results': results,
            'total_found': len(results)
        }
    
    def _extract_keywords(self, question):
        """Extract keywords"""
        stop_words = {'which', 'what', 'where', 'is', 'are', 'the', 'a', 'an', 'in', 'on',
                     'find', 'show', 'me', 'all', 'tables', 'table', 'data', 'information', 'have', 'has'}
        
        words = re.findall(r'\b\w+\b', question.lower())
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        
        return keywords
    
    def close(self):
        self.neo4j.close()


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("BASELINE SYSTEMS TEST")
    print("="*70)
    
    # Initialize all baselines
    keyword_baseline = KeywordSearchBaseline()
    embeddings_baseline = EmbeddingsOnlyBaseline()
    graph_baseline = GraphOnlyBaseline()
    
    # Test query
    test_question = "Which tables contain customer data?"
    
    print(f"\n📋 Test Question: '{test_question}'")
    print("-" * 70)
    
    # Test each baseline
    print("\n1️⃣  KEYWORD SEARCH:")
    result = keyword_baseline.query(test_question, top_k=3)
    for i, r in enumerate(result['results'], 1):
        print(f"   {i}. {r['table']} (score: {r['final_score']})")
    
    print("\n2️⃣  EMBEDDINGS-ONLY:")
    result = embeddings_baseline.query(test_question, top_k=3)
    for i, r in enumerate(result['results'], 1):
        print(f"   {i}. {r['table']} (score: {r['final_score']}%)")
    
    print("\n3️⃣  GRAPH-ONLY:")
    result = graph_baseline.query(test_question, top_k=3)
    for i, r in enumerate(result['results'], 1):
        print(f"   {i}. {r['table']} (score: {r['final_score']})")
    
    print("\n" + "="*70)
    print("✅ All baselines working!")
    
    # Close connections
    keyword_baseline.close()
    embeddings_baseline.close()
    graph_baseline.close()
