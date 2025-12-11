# src/graphrag/unified_llm_graphrag.py

"""
Unified LLM GraphRAG System
UPDATED: Now uses ExplainableGraphRAG for rich cross-source explanations

Routes between:
- metadata (Snowflake) → LangChain (primary) -> Smart GraphRAG (fallback)
- metadata (Databricks) → LangChain (primary) -> Explainable (fallback)
- sample_data → LangChain Text-to-Cypher
- cross_source → ExplainableGraphRAG (NEW - with WHY explanations)
"""

import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

# Updated import - use ExplainableGraphRAG instead of LLMEnhancedSmartGraphRAG
from src.graphrag.explainable_graphrag import ExplainableGraphRAG
from src.graphrag.langchain_graphrag import LangChainGraphRAGEngine
import re
from typing import Dict, Optional


class UnifiedLLMGraphRAG:
    """
    Master system that routes queries to the right engine.
    
    UPDATED Routing (with Explainable support):
    - metadata (Snowflake/general) → LangChain (Cypher) -> Fallback to Explainable
    - databricks → LangChain (Cypher) -> Fallback to Explainable
    - cross_source → ExplainableGraphRAG (primary - rich WHY explanations)
    - sample_data → LangChain Text-to-Cypher
    """
    
    def __init__(self):
        print("🚀 Initializing Unified LLM GraphRAG (with Explainable Cross-Source)...")
        
        self.explainable_engine = None
        self.langchain_engine = None
        self.has_explainable = False
        self.has_langchain = False
        
        # Initialize ExplainableGraphRAG (replaces LLMEnhancedSmartGraphRAG)
        try:
            self.explainable_engine = ExplainableGraphRAG()
            self.has_explainable = True
            print("✅ ExplainableGraphRAG ready (cross-source + Databricks)")
        except Exception as e:
            print(f"⚠️ Explainable engine unavailable: {e}")
        
        # Initialize LangChain (sample data + Cypher generation)
        try:
            self.langchain_engine = LangChainGraphRAGEngine()
            self.has_langchain = True
            print("✅ LangChain Text-to-Cypher ready")
        except Exception as e:
            print(f"⚠️ LangChain unavailable: {e}")
        
        # Statistics
        self.stats = {
            'total_queries': 0,
            'metadata_queries': 0,
            'databricks_queries': 0,
            'cross_source_queries': 0,
            'sample_data_queries': 0,
            'successful': 0,
            'failed': 0,
            'explanations_generated': 0  # NEW: track rich explanations
        }
    
    def classify_query_intent(self, question: str) -> str:
        """
        Enhanced intent classification with Databricks and cross-source support.
        """
        q_lower = question.lower()
        
        # PRIORITY 1: Cross-source queries (use ExplainableGraphRAG)
        cross_source_patterns = [
            r'similar.*(?:across|between|snowflake|databricks)',
            r'(?:snowflake|databricks).{0,20}similar',
            r'cross.{0,10}(?:source|platform|system)',
            r'match.{0,20}(?:between|across)',
            r'databricks.{0,20}(?:like|similar to|match).{0,20}snowflake',
            r'snowflake.{0,20}(?:like|similar to|match).{0,20}databricks',
            r'similar_to',
            r'same data.{0,10}(?:across|in both)',
            r'why.{0,20}similar',  # NEW: "why is X similar to Y"
            r'explain.{0,20}match',  # NEW: "explain this match"
        ]
        if any(re.search(pattern, q_lower) for pattern in cross_source_patterns):
            return 'cross_source'
        
        # PRIORITY 2: Databricks-specific queries
        databricks_keywords = [
            'databricks', 'unity catalog', 'workspace.sample_data',
            'sales_transactions', 'customer_feedback',
            'federated table', 'federated column',
            'sensitivity', 'pii', 'confidential'  # Sensitivity queries
        ]
        if any(kw in q_lower for kw in databricks_keywords):
            return 'databricks'
        
        # PRIORITY 3: Metadata queries (Snowflake)
        metadata_patterns = [
            r'which tables', r'what tables', r'list tables',
            r'find tables', r'show.{0,10}tables',
            r'tables?.{0,10}(?:in|from|with)', 
            r'(?:duplicate|lineage|derives|schema)',
            r'how many (?:tables|columns)',
            r'table.{0,10}(?:row|column|size)',
        ]
        if any(re.search(pattern, q_lower) for pattern in metadata_patterns):
            return 'metadata'
        
        # PRIORITY 4: Sample data queries
        sample_data_patterns = [
            r'customer.{0,15}(?:from|in|city|state)',
            r'orders?.{0,10}(?:delivered|shipped|status)',
            r'product.{0,10}(?:category|furniture|electronics)',
            r'how many (?:customers|orders|products)',
            r'(?:bought|purchased|placed)',
        ]
        if any(re.search(pattern, q_lower) for pattern in sample_data_patterns):
            return 'sample_data'
        
        # Default: treat as metadata
        return 'metadata'
    
    def query(self, nl_question: str, top_k: int = 5) -> Dict:
        """
        Main query entry point with intelligent routing.
        """
        self.stats['total_queries'] += 1
        
        intent = self.classify_query_intent(nl_question)
        
        print(f"\n🎯 Query: '{nl_question}'")
        print(f"   Intent: {intent}")
        
        try:
            if intent == 'cross_source':
                result = self._handle_cross_source_query(nl_question, top_k)
            elif intent == 'databricks':
                result = self._handle_databricks_query(nl_question, top_k)
            elif intent == 'sample_data':
                result = self._handle_sample_data_query(nl_question, top_k)
            else:
                result = self._handle_metadata_query(nl_question, top_k)
            
            self.stats['successful'] += 1
            
            # Track explanation generation
            if result.get('explanation_type'):
                self.stats['explanations_generated'] += 1
            
            return result
            
        except Exception as e:
            self.stats['failed'] += 1
            return self._create_error_response(nl_question, intent, str(e))
    
    def _handle_cross_source_query(self, question: str, top_k: int) -> Dict:
        """
        Handle cross-source queries with ExplainableGraphRAG.
        
        NEW: Provides rich WHY explanations for table similarity.
        """
        self.stats['cross_source_queries'] += 1
        print("🌐 Routing to: ExplainableGraphRAG (cross-source)")
        
        # Use ExplainableGraphRAG for rich explanations
        if self.has_explainable:
            result = self.explainable_engine.query(question, top_k)
            return {
                'question': question,
                'intent': 'cross_source',
                'engine': 'explainable_graphrag',
                'query_type': result.get('query_type', 'cross_source'),
                'explanation_type': result.get('explanation_type', 'cross_source_detailed'),
                'results': result.get('results', []),
                'nl_answer': result.get('nl_answer', ''),
                'success': True
            }
        
        return self._create_error_response(question, 'cross_source', 'No explainable engine available')
    
    def _handle_databricks_query(self, question: str, top_k: int) -> Dict:
        """Handle Databricks queries with rich explanations."""
        self.stats['databricks_queries'] += 1
        print("🧱 Routing to: LangChain Text-to-Cypher (Databricks)")
        
        # Try LangChain first for Cypher generation
        if self.has_langchain:
            try:
                result = self.langchain_engine.query(question, top_k, query_type='databricks')
                
                if result.get('neo4j_results'):
                    return {
                        'question': question,
                        'intent': 'databricks',
                        'engine': 'langchain_databricks',
                        'query_type': result.get('query_type', 'langchain_databricks'),
                        'generated_cypher': result.get('generated_cypher', ''),
                        'neo4j_results': result.get('neo4j_results', []),
                        'result_count': len(result.get('neo4j_results', [])),
                        'nl_answer': result.get('nl_answer', ''),
                        'confidence': 'high',
                        'success': True
                    }
            except Exception as e:
                print(f"   ⚠️ LangChain failed: {e}, falling back to Explainable")
        
        # Fallback to ExplainableGraphRAG
        if self.has_explainable:
            result = self.explainable_engine.query(question, top_k)
            return {
                'question': question,
                'intent': 'databricks',
                'engine': 'explainable_graphrag_fallback',
                'query_type': result.get('query_type', 'databricks_discovery'),
                'explanation_type': result.get('explanation_type'),
                'results': result.get('results', []),
                'nl_answer': result.get('nl_answer', ''),
                'success': True
            }
        
        return self._create_error_response(question, 'databricks', 'No engine available')
    
    def _handle_metadata_query(self, question: str, top_k: int) -> Dict:
        """Handle Snowflake/general metadata queries."""
        self.stats['metadata_queries'] += 1
        print("❄️ Routing to: LangChain Text-to-Cypher (Metadata)")
        
        # Try LangChain first
        if self.has_langchain:
            try:
                result = self.langchain_engine.query(question, top_k, query_type='metadata')
                
                if result.get('neo4j_results'):
                    return {
                        'question': question,
                        'intent': 'metadata',
                        'engine': 'langchain_metadata',
                        'query_type': 'langchain_metadata',
                        'generated_cypher': result.get('generated_cypher', ''),
                        'neo4j_results': result.get('neo4j_results', []),
                        'result_count': len(result.get('neo4j_results', [])),
                        'nl_answer': result.get('nl_answer', ''),
                        'success': True
                    }
            except Exception as e:
                print(f"   ⚠️ LangChain failed: {e}, falling back")
        
        # Fallback to Explainable
        if self.has_explainable:
            result = self.explainable_engine.query(question, top_k)
            return {
                'question': question,
                'intent': 'metadata',
                'engine': 'explainable_graphrag_fallback',
                'query_type': result.get('query_type', 'semantic_discovery'),
                'results': result.get('results', []),
                'nl_answer': result.get('nl_answer', ''),
                'success': True
            }
        
        return self._create_error_response(question, 'metadata', 'No engine available')
    
    def _handle_sample_data_query(self, question: str, top_k: int) -> Dict:
        """Handle sample data queries (Customer, Order, Product)."""
        self.stats['sample_data_queries'] += 1
        print("📊 Routing to: LangChain Text-to-Cypher (Sample Data)")
        
        if self.has_langchain:
            result = self.langchain_engine.query(question, top_k, query_type='sample_data')
            return {
                'question': question,
                'intent': 'sample_data',
                'engine': 'langchain_sample_data',
                'query_type': 'langchain_sample_data',
                'generated_cypher': result.get('generated_cypher', ''),
                'neo4j_results': result.get('neo4j_results', []),
                'result_count': len(result.get('neo4j_results', [])),
                'nl_answer': result.get('nl_answer', ''),
                'success': result.get('success', False)
            }
        
        return self._create_error_response(question, 'sample_data', 'LangChain not available')
    
    def explain_match(self, databricks_table: str, snowflake_table: str) -> str:
        """
        NEW: Dedicated method to explain a specific cross-source match.
        
        Use this for an "Explain" button in the UI.
        
        Args:
            databricks_table: e.g., 'sales_transactions'
            snowflake_table: e.g., 'ORDERS' or 'OLIST_SALES.ORDERS'
        
        Returns:
            Natural language explanation of WHY these tables are similar
        """
        if not self.has_explainable:
            return "Explainable engine not available."
        
        return self.explainable_engine.explain_match(databricks_table, snowflake_table)
    
    def _create_error_response(self, question: str, intent: str, error: str) -> Dict:
        """Create standardized error response."""
        return {
            'question': question,
            'intent': intent,
            'engine': 'error',
            'query_type': 'error',
            'results': [],
            'nl_answer': f"Unable to process query: {error}",
            'success': False,
            'error': error
        }
    
    def get_stats(self) -> Dict:
        """Return query statistics including explanation count."""
        total = self.stats['total_queries']
        return {
            **self.stats,
            'success_rate': self.stats['successful'] / total * 100 if total > 0 else 0,
            'explanation_rate': self.stats['explanations_generated'] / total * 100 if total > 0 else 0
        }
    
    def close(self):
        """Clean up resources."""
        print("\n🔒 Closing Unified LLM GraphRAG...")
        
        if self.has_explainable and self.explainable_engine:
            try:
                self.explainable_engine.close()
                print("✅ Explainable engine closed")
            except Exception as e:
                print(f"⚠️ Error closing Explainable engine: {e}")
        
        if self.has_langchain and self.langchain_engine:
            try:
                if hasattr(self.langchain_engine, 'graph'):
                    self.langchain_engine.graph._driver.close()
                print("✅ LangChain engine closed")
            except Exception as e:
                print(f"⚠️ Error closing LangChain: {e}")
        
        print("🔒 Cleanup complete")


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("🧪 TESTING UNIFIED LLM GRAPHRAG (with Explainable)")
    print("="*70)
    
    engine = UnifiedLLMGraphRAG()
    
    test_queries = [
        # Cross-source (should use ExplainableGraphRAG)
        ("Find cross-source matches", "cross_source"),
        ("Which Databricks tables are similar to Snowflake?", "cross_source"),
        ("Why is sales_transactions similar to ORDERS?", "cross_source"),
        
        # Databricks
        ("Show me Databricks tables", "databricks"),
        ("Which columns have high sensitivity?", "databricks"),
        
        # Metadata
        ("Which tables have the most rows?", "metadata"),
        ("Show duplicate tables", "metadata"),
        
        # Sample data
        ("How many customers from São Paulo?", "sample_data"),
    ]
    
    for query, expected_intent in test_queries:
        print(f"\n{'='*70}")
        print(f"Query: '{query}'")
        print(f"Expected: {expected_intent}")
        print("-"*70)
        
        result = engine.query(query, top_k=5)
        
        print(f"Actual intent: {result.get('intent', 'unknown')}")
        print(f"Engine: {result.get('engine', 'unknown')}")
        print(f"Explanation type: {result.get('explanation_type', 'none')}")
        print(f"\n💬 Answer:\n{result.get('nl_answer', 'No answer')[:500]}")
    
    # Test dedicated explain_match
    print(f"\n{'='*70}")
    print("Testing explain_match() API:")
    print("-"*70)
    
    explanation = engine.explain_match("sales_transactions", "ORDERS")
    print(f"\n💬 Explanation:\n{explanation}")
    
    # Print stats
    print(f"\n{'='*70}")
    print("📊 Query Statistics:")
    stats = engine.get_stats()
    for k, v in stats.items():
        print(f"   {k}: {v}")
    
    print(f"\n{'='*70}")
    print("✅ Testing Complete!")
    print("="*70)
    
    engine.close()