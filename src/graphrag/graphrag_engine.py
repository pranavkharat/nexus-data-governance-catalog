from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

class GraphRAGEngine:
    """
    Hybrid retrieval engine combining:
    - Semantic search (Milvus vector similarity)
    - Graph traversal (Neo4j relationship context)
    - Weighted scoring: 60% semantic + 40% structural
    """
    
    def __init__(self):
        # Connect to Milvus
        connections.connect(host='localhost', port='19530')
        self.collection = Collection("table_metadata")
        self.collection.load()
        print("✅ Connected to Milvus")
        
        # Load embedding model
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✅ Loaded embedding model")
        
        # Connect to Neo4j
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        self.neo4j = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        print("✅ Connected to Neo4j")
    
    def query(self, nl_question: str, top_k: int = 5):
        """
        Main GraphRAG query function
        
        Args:
            nl_question: Natural language question
            top_k: Number of results to return
        
        Returns:
            Dict with ranked results and explanations
        """
        print(f"\n🔍 GraphRAG Query: '{nl_question}'")
        print("=" * 70)
        
        # Step 1: Semantic search in Milvus
        print("📊 Step 1: Semantic search (Milvus)...")
        semantic_results = self._semantic_search(nl_question, top_k=10)
        print(f"   ✓ Found {len(semantic_results)} semantic matches")
        
        # Step 2: Get graph context from Neo4j
        print("🌐 Step 2: Graph context retrieval (Neo4j)...")
        graph_context = self._get_graph_context(semantic_results)
        print(f"   ✓ Retrieved graph context for {len(graph_context)} tables")
        
        # Step 3: Hybrid ranking (70% semantic + 30% structural)
        print("⚖️  Step 3: Hybrid ranking (70% semantic + 30% structural)...")
        ranked_results = self._hybrid_rank(semantic_results, graph_context)
        print(f"   ✓ Ranked {len(ranked_results)} results")
        
        print("=" * 70)
        
        return {
            'question': nl_question,
            'results': ranked_results[:top_k],
            'total_found': len(ranked_results)
        }
    
    def _semantic_search(self, question, top_k=10):
        """Vector similarity search in Milvus"""
        
        # Generate query embedding
        query_embedding = self.model.encode(question).tolist()
        
        # Search in Milvus
        results = self.collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k,
            output_fields=["text"]
        )
        
        return results[0]  # Return first batch
    
    def _get_graph_context(self, semantic_results):
        """Get graph centrality and relationships from Neo4j"""
        
        # Extract table names from semantic results
        table_names = []
        for hit in semantic_results:
            text = hit.entity.get('text')
            # Parse "SCHEMA.TABLE (rows)" format
            table_part = text.split(' (')[0]  # Get "SCHEMA.TABLE"
            table_name = table_part.split('.')[1] if '.' in table_part else table_part
            table_names.append(table_name)
        
        # Query Neo4j for graph context
        with self.neo4j.session() as session:
            result = session.run("""
                MATCH (t:OlistData)
                WHERE t.name IN $table_names
                OPTIONAL MATCH (t)-[r]-(related:OlistData)
                WITH t, count(DISTINCT r) as centrality, collect(DISTINCT related.name)[..3] as neighbors
                RETURN t.name as table,
                       t.schema as schema,
                       t.row_count as rows,
                       centrality,
                       neighbors
            """, table_names=table_names)
            
            # Convert to dict keyed by table name
            graph_data = {}
            for record in result:
                graph_data[record['table']] = {
                    'schema': record['schema'],
                    'rows': record['rows'],
                    'centrality': record['centrality'],
                    'neighbors': record['neighbors']
                }
            
            return graph_data
    
    def _hybrid_rank(self, semantic_results, graph_context, semantic_weight=0.7, structural_weight=0.3):
        """
        Combine semantic similarity + graph centrality
        Formula: final_score = semantic_weight * semantic + structural_weight * structural
        
        Default: 70% semantic + 30% structural (tuned empirically)
        Previous 60-40 caused hub tables (ORDERS) to dominate all queries
        """
        
        ranked = []
        
        for hit in semantic_results:
            # Parse table info from text
            text = hit.entity.get('text')
            table_part = text.split(' (')[0]
            schema_name = table_part.split('.')[0] if '.' in table_part else 'UNKNOWN'
            table_name = table_part.split('.')[1] if '.' in table_part else table_part
            
            # Semantic score from Milvus (cosine similarity)
            semantic_score = hit.distance
            
            # Structural score from Neo4j (normalized centrality)
            graph_data = graph_context.get(table_name, {})
            centrality = graph_data.get('centrality', 0)
            
            # Logarithmic normalization to reduce hub dominance
            import math
            if centrality > 0:
                structural_score = math.log(centrality + 1) / math.log(7)  # log scale normalization
            else:
                structural_score = 0.0
            
            # Weighted combination: 70% semantic + 30% structural (empirically tuned)
            final_score = (semantic_weight * semantic_score) + (structural_weight * structural_score)
            
            ranked.append({
                'table': f"{schema_name}.{table_name}",
                'rows': graph_data.get('rows', 0),
                'semantic_score': round(semantic_score * 100, 1),
                'structural_score': round(structural_score * 100, 1),
                'final_score': round(final_score * 100, 1),
                'centrality': centrality,
                'neighbors': graph_data.get('neighbors', []),
                'reasoning': f"semantic ({semantic_score:.3f}) + centrality ({structural_score:.3f})"
            })
        
        # Sort by final score descending
        return sorted(ranked, key=lambda x: x['final_score'], reverse=True)
    
    def close(self):
        """Close connections"""
        self.neo4j.close()

# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    
    engine = GraphRAGEngine()
    
    print("\n" + "🎯" * 35)
    print("GRAPHRAG HYBRID RETRIEVAL DEMO")
    print("🎯" * 35)
    
    # Test queries
    test_queries = [
        "Which tables contain customer data?",
        "Where is payment information stored?",
        "Find product catalog tables",
        "Show me order and purchase tables"
    ]
    
    for query in test_queries:
        result = engine.query(query, top_k=5)
        
        print(f"\n📊 Results for: '{result['question']}'")
        print("-" * 70)
        print(f"{'Rank':<6} {'Table':<35} {'Semantic':<10} {'Structural':<12} {'Final':<8}")
        print("-" * 70)
        
        for i, r in enumerate(result['results'], 1):
            print(f"{i:<6} {r['table']:<35} {r['semantic_score']}%{'':<6} {r['structural_score']}%{'':<8} {r['final_score']}%")
        
        print()
        
        # Show reasoning for top result
        top = result['results'][0]
        print(f"💡 Top Result Reasoning:")
        print(f"   {top['table']} ({top['rows']:,} rows)")
        print(f"   Semantic match: {top['semantic_score']}%")
        print(f"   Graph centrality: {top['structural_score']}% ({top['centrality']} relationships)")
        if top['neighbors']:
            print(f"   Connected to: {', '.join(top['neighbors'])}")
        print(f"   Final score: {top['final_score']}%")
        print()
    
    engine.close()
    
    print("=" * 70)
    print("✅ GraphRAG Demo Complete!")
    print("=" * 70)