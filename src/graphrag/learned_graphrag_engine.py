"""
GraphRAG with XGBoost-based adaptive routing
Learns optimal routing strategy from data instead of using fixed rules
"""
# Add project root to path
import sys
from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)


import joblib
import numpy as np
from dotenv import load_dotenv
from src.graphrag.query_features import QueryFeatureExtractor
import json

load_dotenv()

class LearnedGraphRAGEngine:
    """
    GraphRAG with ML-based routing instead of rules
    Uses XGBoost to predict optimal route for each query
    """
    
    def __init__(self):
        # Connect to databases
        connections.connect(host='localhost', port='19530')
        self.collection = Collection("table_metadata")
        self.collection.load()
        print("✅ Connected to Milvus")
        
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✅ Loaded embedding model")
        
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        self.neo4j = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        print("✅ Connected to Neo4j")
        
        # Load trained XGBoost model
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        model_path = os.path.join(project_root, 'models', 'route_classifier.pkl')
        encoder_path = os.path.join(project_root, 'models', 'label_encoder.pkl')
        features_path = os.path.join(project_root, 'models', 'feature_names.json')
        
        self.route_classifier = joblib.load(model_path)
        self.label_encoder = joblib.load(encoder_path)
        
        with open(features_path, 'r') as f:
            self.feature_names = json.load(f)
        
        print("✅ Loaded XGBoost route classifier")
        
        # Feature extractor
        self.feature_extractor = QueryFeatureExtractor()
    
    def predict_route(self, question: str):
        """Predict best route using XGBoost"""
        
        # Extract features
        features_dict = self.feature_extractor.extract_features(question)
        
        # Convert to array in correct order
        features_array = np.array([features_dict.get(name, 0.0) for name in self.feature_names])
        features_array = features_array.reshape(1, -1)
        
        # Predict route
        route_idx = self.route_classifier.predict(features_array)[0]
        route = self.label_encoder.inverse_transform([route_idx])[0]
        
        # Get confidence scores
        route_probs = self.route_classifier.predict_proba(features_array)[0]
        confidence = route_probs.max()
        
        # Get all route probabilities
        route_probabilities = {
            self.label_encoder.inverse_transform([i])[0]: prob 
            for i, prob in enumerate(route_probs)
        }
        
        return route, confidence, route_probabilities
    
    def query(self, nl_question: str, top_k: int = 5):
        """Main query interface with learned routing"""
        
        print(f"\n🔍 Question: '{nl_question}'")
        
        # Predict route using ML
        route, confidence, route_probs = self.predict_route(nl_question)
        
        print(f"🤖 ML Predicted Route: {route} (confidence: {confidence:.3f})")
        print(f"   Route probabilities:")
        for r, p in sorted(route_probs.items(), key=lambda x: x[1], reverse=True):
            print(f"      {r}: {p:.3f}")
        print("=" * 70)
        
        # Execute the predicted route (use same handlers as SmartGraphRAG)
        if route == 'metadata_filter':
            results = self._metadata_filter_query(nl_question, top_k)
        elif route == 'duplicate_detection':
            results = self._duplicate_query(nl_question, top_k)
        elif route == 'relationship_traversal':
            results = self._relationship_query(nl_question, top_k)
        else:  # semantic_discovery
            results = self._hybrid_search_query(nl_question, top_k)
        
        print("=" * 70)
        
        return {
            'question': nl_question,
            'query_type': route,
            'ml_confidence': confidence,
            'route_probabilities': route_probs,
            'results': results,
            'total_found': len(results)
        }
    
    # ========================================
    # ROUTE HANDLERS (Same as SmartGraphRAG)
    # ========================================
    
    def _metadata_filter_query(self, question, top_k):
        """Metadata filtering"""
        q_lower = question.lower()
        
        with self.neo4j.session() as session:
            if 'most rows' in q_lower or 'largest' in q_lower or 'more than' in q_lower:
                result = session.run("""
                    MATCH (t:OlistData)
                    WHERE t.row_count IS NOT NULL
                    RETURN t.schema + '.' + t.name as table, t.row_count as rows,
                           100.0 as semantic_score, 50.0 as structural_score, 100.0 as final_score,
                           0 as centrality, [] as neighbors, 'metadata_filter' as reasoning
                    ORDER BY t.row_count DESC LIMIT $top_k
                """, top_k=top_k)
            
            elif 'smallest' in q_lower or 'fewest' in q_lower:
                result = session.run("""
                    MATCH (t:OlistData)
                    WHERE t.row_count IS NOT NULL AND t.row_count > 0
                    RETURN t.schema + '.' + t.name as table, t.row_count as rows,
                           100.0 as semantic_score, 50.0 as structural_score, 100.0 as final_score,
                           0 as centrality, [] as neighbors, 'metadata_filter' as reasoning
                    ORDER BY t.row_count ASC LIMIT $top_k
                """, top_k=top_k)
            
            elif any(schema in q_lower for schema in ['olist_sales', 'olist_marketing', 'olist_analytics']):
                schema_name = 'OLIST_SALES' if 'olist_sales' in q_lower else \
                             'OLIST_MARKETING' if 'olist_marketing' in q_lower else 'OLIST_ANALYTICS'
                
                result = session.run("""
                    MATCH (t:OlistData {schema: $schema})
                    RETURN t.schema + '.' + t.name as table, t.row_count as rows,
                           100.0 as semantic_score, 50.0 as structural_score, 100.0 as final_score,
                           0 as centrality, [] as neighbors, 'metadata_filter' as reasoning
                    ORDER BY t.name
                """, schema=schema_name)
            
            else:
                return []
            
            return [dict(record) for record in result]
    
    def _duplicate_query(self, question, top_k):
        """Duplicate detection"""
        with self.neo4j.session() as session:
            result = session.run("""
                MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData)
                RETURN t2.schema + '.' + t2.name as table, t2.row_count as rows,
                       100.0 as semantic_score, 50.0 as structural_score, 100.0 as final_score,
                       0 as centrality, [t1.schema + '.' + t1.name] as neighbors,
                       'duplicate_detection' as reasoning
                LIMIT $top_k
            """, top_k=top_k)
            
            return [dict(record) for record in result]
    
    def _relationship_query(self, question, top_k):
        """Relationship traversal"""
        q_lower = question.lower()
        
        table_map = {
            'customers': 'CUSTOMERS', 'orders': 'ORDERS',
            'order_items': 'ORDER_ITEMS', 'products': 'PRODUCTS',
            'sellers': 'SELLERS', 'payments': 'ORDER_PAYMENTS',
            'reviews': 'ORDER_REVIEWS', 'geolocation': 'GEOLOCATION'
        }
        
        target_table = None
        for keyword, table_name in table_map.items():
            if keyword in q_lower:
                target_table = table_name
                break
        
        if not target_table:
            return []
        
        with self.neo4j.session() as session:
            result = session.run("""
                MATCH (t:OlistData {name: $table})-[r]-(related:OlistData)
                RETURN DISTINCT related.schema + '.' + related.name as table,
                       related.row_count as rows,
                       50.0 as semantic_score, 100.0 as structural_score, 80.0 as final_score,
                       1 as centrality, [type(r)] as neighbors,
                       'relationship_traversal' as reasoning
                LIMIT $top_k
            """, table=target_table, top_k=top_k)
            
            return [dict(record) for record in result]
    
    def _hybrid_search_query(self, question, top_k):
        """Hybrid GraphRAG (80/20 weighting)"""
        semantic_results = self._semantic_search(question, top_k=10)
        graph_context = self._get_graph_context(semantic_results)
        ranked_results = self._hybrid_rank(semantic_results, graph_context)
        
        return ranked_results[:top_k]
    
    def _semantic_search(self, question, top_k=10):
        """Vector search"""
        query_embedding = self.model.encode(question).tolist()
        results = self.collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k,
            output_fields=["text"]
        )
        return results[0]
    
    def _get_graph_context(self, semantic_results):
        """Get graph context"""
        table_names = []
        for hit in semantic_results:
            text = hit.entity.get('text')
            table_part = text.split(' (')[0]
            table_name = table_part.split('.')[1] if '.' in table_part else table_part
            table_names.append(table_name)
        
        with self.neo4j.session() as session:
            result = session.run("""
                MATCH (t:OlistData)
                WHERE t.name IN $table_names
                OPTIONAL MATCH (t)-[r]-(related:OlistData)
                WITH t, count(DISTINCT r) as centrality, collect(DISTINCT related.name)[..3] as neighbors
                RETURN t.name as table, t.schema as schema, t.row_count as rows, centrality, neighbors
            """, table_names=table_names)
            
            return {record['table']: dict(record) for record in result}
    
    def _hybrid_rank(self, semantic_results, graph_context):
        """80% semantic + 20% structural"""
        import math
        
        ranked = []
        
        for hit in semantic_results:
            text = hit.entity.get('text')
            table_part = text.split(' (')[0]
            schema_name = table_part.split('.')[0] if '.' in table_part else 'UNKNOWN'
            table_name = table_part.split('.')[1] if '.' in table_part else table_part
            
            semantic_score = hit.distance
            
            graph_data = graph_context.get(table_name, {})
            centrality = graph_data.get('centrality', 0)
            structural_score = math.log(centrality + 1) / math.log(7) if centrality > 0 else 0.0
            
            final_score = (0.8 * semantic_score) + (0.2 * structural_score)
            
            ranked.append({
                'table': f"{schema_name}.{table_name}",
                'rows': graph_data.get('rows', 0),
                'semantic_score': round(semantic_score * 100, 1),
                'structural_score': round(structural_score * 100, 1),
                'final_score': round(final_score * 100, 1),
                'centrality': centrality,
                'neighbors': graph_data.get('neighbors', []),
                'reasoning': f"hybrid: semantic ({semantic_score:.3f}) + centrality ({structural_score:.3f})"
            })
        
        return sorted(ranked, key=lambda x: x['final_score'], reverse=True)
    
    def close(self):
        self.neo4j.close()


# ========================================
# TESTING
# ========================================

if __name__ == "__main__":
    
    print("\n" + "🤖" * 35)
    print("LEARNED GRAPHRAG TEST")
    print("🤖" * 35)
    
    engine = LearnedGraphRAGEngine()
    
    test_queries = [
        "Which tables contain customer data?",
        "Show me all duplicate tables",
        "What tables connect to ORDERS?",
        "Which tables have more than 100,000 rows?",
    ]
    
    for query in test_queries:
        result = engine.query(query, top_k=3)
        
        print(f"\n📋 Results:")
        for i, r in enumerate(result['results'], 1):
            print(f"   {i}. {r['table']} (score: {r['final_score']}%)")
    
    engine.close()