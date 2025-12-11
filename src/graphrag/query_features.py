
"""
Query Feature Extraction for ML-Based Routing
Extracts 25+ features from natural language questions
"""

from sentence_transformers import SentenceTransformer
import re
from typing import Dict, List
import numpy as np

class QueryFeatureExtractor:
    """Extract features from queries for ML classification"""
    
    def __init__(self):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """
        Extract comprehensive features from query
        
        Returns dict with 30+ features for ML classification
        """
        features = {}
        query_lower = query.lower()
        tokens = query_lower.split()
        
        # ============================================
        # 1. LEXICAL FEATURES (Simple but Powerful)
        # ============================================
        
        features['query_length'] = len(tokens)
        features['char_length'] = len(query)
        features['avg_word_length'] = np.mean([len(w) for w in tokens]) if tokens else 0
        features['has_question_mark'] = float('?' in query)
        
        # ============================================
        # 2. KEYWORD PRESENCE (Binary Indicators)
        # ============================================
        
        # Duplicate keywords
        duplicate_keywords = ['duplicate', 'copy', 'copies', 'same as', 'identical', 
                             'exact', 'versions of', 'renamed']
        features['has_duplicate_kw'] = float(any(kw in query_lower for kw in duplicate_keywords))
        
        # Relationship keywords
        relationship_keywords = ['join', 'connect', 'link', 'relate', 'reference', 
                                'foreign key', 'linked to', 'upstream', 'downstream']
        features['has_relationship_kw'] = float(any(kw in query_lower for kw in relationship_keywords))
        
        # Metadata keywords
        metadata_keywords = ['rows', 'large', 'small', 'size', 'count', 'most', 
                            'largest', 'smallest', 'columns', 'schema']
        features['has_metadata_kw'] = float(any(kw in query_lower for kw in metadata_keywords))
        
        # Discovery keywords
        discovery_keywords = ['find', 'which', 'show', 'search', 'contain', 
                             'where', 'list', 'get']
        features['has_discovery_kw'] = float(any(kw in query_lower for kw in discovery_keywords))
        
        # ============================================
        # 3. QUESTION TYPE INDICATORS
        # ============================================
        
        features['starts_with_which'] = float(query_lower.startswith('which'))
        features['starts_with_what'] = float(query_lower.startswith('what'))
        features['starts_with_where'] = float(query_lower.startswith('where'))
        features['starts_with_show'] = float(query_lower.startswith('show'))
        features['starts_with_find'] = float(query_lower.startswith('find'))
        
        # ============================================
        # 4. SPECIFICITY INDICATORS
        # ============================================
        
        # Mentions specific table names (usually uppercase)
        features['mentions_table_name'] = float(any(token.isupper() for token in query.split()))
        
        # Contains numbers (e.g., ">100000 rows")
        features['contains_number'] = float(bool(re.search(r'\d+', query)))
        
        # Contains comparison operators
        features['has_comparison'] = float(any(op in query for op in ['>', '<', '>=', '<=', '=']))
        
        # ============================================
        # 5. SEMANTIC FEATURES (Embedding-based)
        # ============================================
        
        # Get query embedding
        embedding = self.model.encode(query)
        
        # Use first 10 principal dimensions (avoid overfitting with all 384)
        for i in range(10):
            features[f'embed_dim_{i}'] = float(embedding[i])
        
        # ============================================
        # 6. COMPLEXITY INDICATORS
        # ============================================
        
        features['num_commas'] = float(query.count(','))
        features['num_and'] = float(query_lower.count(' and '))
        features['num_or'] = float(query_lower.count(' or '))
        
        return features
    
    def extract_batch(self, queries: List[str]) -> List[Dict[str, float]]:
        """Extract features for multiple queries"""
        return [self.extract_features(q) for q in queries]


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    extractor = QueryFeatureExtractor()
    
    test_queries = [
        "Which tables contain customer data?",
        "Find all duplicate tables",
        "What tables connect to ORDERS?",
        "Which tables have more than 100000 rows?",
        "Show me tables in OLIST_ANALYTICS schema"
    ]
    
    print("Testing Feature Extraction")
    print("=" * 70)
    
    for query in test_queries:
        features = extractor.extract_features(query)
        print(f"\nQuery: '{query}'")
        print(f"Features extracted: {len(features)}")
        print(f"Sample features:")
        print(f"  - query_length: {features['query_length']}")
        print(f"  - has_duplicate_kw: {features['has_duplicate_kw']}")
        print(f"  - has_relationship_kw: {features['has_relationship_kw']}")
        print(f"  - has_metadata_kw: {features['has_metadata_kw']}")
        print(f"  - starts_with_which: {features['starts_with_which']}")
    
    print("\n✅ Feature extraction working!")