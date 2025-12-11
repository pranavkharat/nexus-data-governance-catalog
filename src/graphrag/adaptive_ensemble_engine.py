# src/graphrag/adaptive_ensemble_engine.py

"""
Adaptive Ensemble GraphRAG: XGBoost + Smart Routing
Uses XGBoost confidence to decide: single route vs multi-route
"""

from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import os
import math
import joblib
import json
import numpy as np
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

class AdaptiveEnsembleEngine:
    """
    Intelligent adaptive system:
    - High XGBoost confidence (>80%) → Run only predicted route
    - Low confidence → Run top 2 routes and merge
    """
    
    def __init__(self):
        # Database connections
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
        
        # Load XGBoost model
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            model_path = os.path.join(project_root, 'models', 'route_classifier.pkl')
            encoder_path = os.path.join(project_root, 'models', 'label_encoder.pkl')
            features_path = os.path.join(project_root, 'models', 'feature_names.json')
            
            self.route_classifier = joblib.load(model_path)
            self.label_encoder = joblib.load(encoder_path)
            
            with open(features_path, 'r') as f:
                self.feature_names = json.load(f)
            
            from src.graphrag.query_features import QueryFeatureExtractor
            self.feature_extractor = QueryFeatureExtractor()
            
            self.has_ml = True
            print("✅ Loaded XGBoost route classifier")
        except Exception as e:
            print(f"⚠️  XGBoost not available: {e}")
            self.has_ml = False
    
    def predict_adaptive_weights(self, question):
        """Get XGBoost probability distribution"""
        
        if not self.has_ml:
            return {
                'semantic_discovery': 0.40,
                'metadata_filter': 0.25,
                'duplicate_detection': 0.20,
                'relationship_traversal': 0.15
            }
        
        features_dict = self.feature_extractor.extract_features(question)
        features_array = np.array([features_dict.get(name, 0.0) for name in self.feature_names]).reshape(1, -1)
        
        route_probs = self.route_classifier.predict_proba(features_array)[0]
        
        weights = {}
        for i, route_name in enumerate(self.label_encoder.classes_):
            weights[route_name] = float(route_probs[i])
        
        return weights
    
    def query(self, nl_question: str, top_k: int = 5):
        """
        Adaptive query with confidence-based strategy selection
        """
        
        # Get weights
        weights = self.predict_adaptive_weights(nl_question)
        max_route = max(weights.items(), key=lambda x: x[1])
        
        # High confidence → Single route
        if max_route[1] > 0.80:
            return self._execute_single_route(nl_question, max_route[0], weights, top_k)
        else:
            return self._execute_multi_route(nl_question, weights, top_k)
    
    def _execute_single_route(self, question, route_name, weights, top_k):
        """Execute only the predicted route"""
        
        if route_name == 'semantic_discovery':
            results = self._semantic_route(question, top_k)
        elif route_name == 'metadata_filter':
            results = self._metadata_route(question, top_k)
        elif route_name == 'duplicate_detection':
            results = self._duplicate_route(question, top_k)
        else:
            results = self._relationship_route(question, top_k)
        
        # Format
        formatted = []
        for i, r in enumerate(results[:top_k], 1):
            formatted.append({
                'table': r.get('table'),
                'rows': r.get('rows', 0),
                'final_score': round((11-i) * 10, 1),
                'routes_found_in': [route_name]
            })
        
        return {
            'question': question,
            'query_type': route_name,
            'weights_used': weights,
            'results': formatted
        }
    
    def _execute_multi_route(self, question, weights, top_k):
        """Execute top 2 routes and merge"""
        
        sorted_routes = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:2]
        
        route_results = {}
        for route, weight in sorted_routes:
            if route == 'semantic_discovery':
                route_results[route] = self._semantic_route(question, 10)
            elif route == 'metadata_filter':
                route_results[route] = self._metadata_route(question, 10)
            elif route == 'duplicate_detection':
                route_results[route] = self._duplicate_route(question, 10)
            else:
                route_results[route] = self._relationship_route(question, 10)
        
        merged = self._simple_merge(route_results, weights)
        
        return {
            'question': question,
            'query_type': 'multi_route',
            'weights_used': weights,
            'results': merged[:top_k]
        }
    
    def _simple_merge(self, route_results, weights):
        """Simple merge without consensus bonus"""
        
        table_scores = {}
        
        for route, results in route_results.items():
            route_weight = weights.get(route, 0)
            
            for i, result in enumerate(results[:5], start=1):
                table = result.get('table')
                if not table:
                    continue
                
                rank_score = (6 - i) / 5
                score = rank_score * route_weight
                
                if table not in table_scores:
                    table_scores[table] = {'table': table, 'rows': result.get('rows', 0), 'score': 0}
                
                table_scores[table]['score'] += score
        
        sorted_results = sorted(table_scores.values(), key=lambda x: x['score'], reverse=True)
        
        return [{'table': r['table'], 'rows': r['rows'], 'final_score': round(r['score']*100, 1)} 
                for r in sorted_results]
    
    # ========================================
    # ROUTE IMPLEMENTATIONS (COMPLETE!)
    # ========================================
    
    def _semantic_route(self, question, top_k):
        """Semantic discovery with hybrid ranking"""
        try:
            # Vector search
            query_embedding = self.model.encode(question).tolist()
            milvus_results = self.collection.search(
                data=[query_embedding],
                anns_field="embedding",
                param={"metric_type": "COSINE", "params": {"ef": 64}},
                limit=top_k,
                output_fields=["text"]
            )
            
            # Extract table names and get graph context
            results = []
            for hit in milvus_results[0]:
                text = hit.entity.get('text', '')
                table_part = text.split(' (')[0]
                
                # Parse schema.table
                if '.' in table_part:
                    schema = table_part.split('.')[0]
                    table_name = table_part.split('.')[1]
                else:
                    schema = 'UNKNOWN'
                    table_name = table_part
                
                # Get row count from text or query Neo4j
                rows = 0
                if '(' in text:
                    rows_text = text.split('(')[1].replace(' rows)', '').strip()
                    try:
                        rows = int(rows_text)
                    except:
                        rows = 0
                
                results.append({
                    'table': f"{schema}.{table_name}",
                    'rows': rows
                })
            
            return results
            
        except Exception as e:
            print(f"⚠️  Semantic route error: {e}")
            return []
    
    def _metadata_route(self, question, top_k):
        """Metadata filtering"""
        try:
            q_lower = question.lower()
            
            with self.neo4j.session() as session:
                
                # Largest tables
                if 'most rows' in q_lower or 'largest' in q_lower or 'more than' in q_lower:
                    result = session.run("""
                        MATCH (t:OlistData)
                        WHERE t.row_count IS NOT NULL
                        RETURN t.schema + '.' + t.name as table, t.row_count as rows
                        ORDER BY t.row_count DESC LIMIT $top_k
                    """, top_k=top_k)
                    return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
                # Smallest tables
                elif 'smallest' in q_lower or 'fewest' in q_lower or 'less than' in q_lower:
                    result = session.run("""
                        MATCH (t:OlistData)
                        WHERE t.row_count IS NOT NULL AND t.row_count > 0
                        RETURN t.schema + '.' + t.name as table, t.row_count as rows
                        ORDER BY t.row_count ASC LIMIT $top_k
                    """, top_k=top_k)
                    return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
                # Schema filtering
                elif 'olist_sales' in q_lower:
                    result = session.run("""
                        MATCH (t:OlistData {schema: 'OLIST_SALES'})
                        RETURN t.schema + '.' + t.name as table, t.row_count as rows
                    """)
                    return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
                elif 'olist_marketing' in q_lower:
                    result = session.run("""
                        MATCH (t:OlistData {schema: 'OLIST_MARKETING'})
                        RETURN t.schema + '.' + t.name as table, t.row_count as rows
                    """)
                    return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
                elif 'olist_analytics' in q_lower:
                    result = session.run("""
                        MATCH (t:OlistData {schema: 'OLIST_ANALYTICS'})
                        RETURN t.schema + '.' + t.name as table, t.row_count as rows
                    """)
                    return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
                else:
                    return []
                    
        except Exception as e:
            print(f"⚠️  Metadata route error: {e}")
            return []
    
    def _duplicate_route(self, question, top_k):
        """Duplicate detection"""
        try:
            with self.neo4j.session() as session:
                result = session.run("""
                    MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]-(t2:OlistData)
                    RETURN DISTINCT t2.schema + '.' + t2.name as table, 
                           t2.row_count as rows
                    LIMIT $top_k
                """, top_k=top_k)
                
                return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
        except Exception as e:
            print(f"⚠️  Duplicate route error: {e}")
            return []
    
    def _relationship_route(self, question, top_k):
        """Relationship traversal"""
        try:
            q_lower = question.lower()
            
            # Extract mentioned table
            table_map = {
                'customers': 'CUSTOMERS', 'customer': 'CUSTOMERS',
                'orders': 'ORDERS', 'order': 'ORDERS',
                'order_items': 'ORDER_ITEMS', 'items': 'ORDER_ITEMS',
                'products': 'PRODUCTS', 'product': 'PRODUCTS',
                'sellers': 'SELLERS', 'seller': 'SELLERS',
                'payments': 'ORDER_PAYMENTS', 'payment': 'ORDER_PAYMENTS',
                'reviews': 'ORDER_REVIEWS', 'review': 'ORDER_REVIEWS',
                'geolocation': 'GEOLOCATION'
            }
            
            target_table = None
            for keyword, table_name in table_map.items():
                if keyword in q_lower:
                    target_table = table_name
                    break
            
            if not target_table:
                # No specific table, return empty
                return []
            
            with self.neo4j.session() as session:
                result = session.run("""
                    MATCH (t:OlistData {name: $table})-[r]-(related:OlistData)
                    RETURN DISTINCT related.schema + '.' + related.name as table,
                           related.row_count as rows
                    LIMIT $top_k
                """, table=target_table, top_k=top_k)
                
                return [{'table': r['table'], 'rows': r['rows']} for r in result]
                
        except Exception as e:
            print(f"⚠️  Relationship route error: {e}")
            return []
    
    def close(self):
        """Close connections"""
        self.neo4j.close()


# ========================================
# TESTING
# ========================================

if __name__ == "__main__":
    
    print("\n" + "🎯" * 35)
    print("ADAPTIVE ENSEMBLE V2 TEST")
    print("🎯" * 35)
    
    engine = AdaptiveEnsembleEngine()
    
    test_queries = [
        "Which tables contain customer data?",
        "Show me all duplicate tables",
        "What tables connect to ORDERS?",
        "Which tables have more than 100,000 rows?",
    ]
    
    for query in test_queries:
        try:
            result = engine.query(query, top_k=5)
            
            print(f"\n📋 Results for: '{query}'")
            print(f"   Strategy: {result['query_type']}")
            
            for i, r in enumerate(result['results'], 1):
                print(f"   {i}. {r['table']}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    engine.close()
    print("\n✅ Test complete")