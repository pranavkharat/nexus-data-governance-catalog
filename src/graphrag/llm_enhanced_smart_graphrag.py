# src/graphrag/llm_enhanced_smart_graphrag.py

"""
LLM-Enhanced Smart GraphRAG
Combines: Smart routing (60% accuracy) + Ollama natural language answers

IMPROVEMENTS:
- Better prompt engineering to avoid "I don't know"
- Response validation with fallback
- Richer context building
- Configurable LLM parameters
"""

from .smart_graphrag_engine import SmartGraphRAGEngine
from langchain_ollama import ChatOllama
from typing import Dict, List, Optional
import os


class LLMEnhancedSmartGraphRAG(SmartGraphRAGEngine):
    """
    Extends Smart GraphRAG with natural language explanations.
    Uses proven Smart routing (60% accuracy), adds LLM for user-friendly answers.
    
    Key insight: Symbolic routing for retrieval accuracy + Neural for explanation quality
    """
    
    def __init__(self, temperature: float = 0.3):
        """
        Initialize LLM-Enhanced Smart GraphRAG.
        
        Args:
            temperature: LLM temperature for answer generation (0.0-1.0)
                        Lower = more focused, Higher = more creative
        """
        # Initialize base Smart GraphRAG (handles Neo4j + Milvus)
        super().__init__()
        
        self.temperature = temperature
        self.llm = None
        self.has_llm = False
        
        # Initialize Ollama LLM
        try:
            self.llm = ChatOllama(
                model="llama3.1",
                base_url="http://localhost:11434",
                temperature=temperature
            )
            # Test connection
            test_response = self.llm.invoke("Say OK")
            if test_response:
                self.has_llm = True
                print(f"✅ Ollama LLM initialized (temp={temperature})")
        except Exception as e:
            print(f"⚠️ Ollama not available: {e}")
            print("   Will use template-based answers instead")
    
    def query(self, nl_question: str, top_k: int = 5) -> Dict:
        """
        Smart GraphRAG retrieval + LLM natural language answer.
        
        Args:
            nl_question: Natural language question
            top_k: Number of results to return
            
        Returns:
            Dict with results, nl_answer, and metadata
        """
        # Step 1: Use Smart GraphRAG for retrieval (proven 60% accuracy)
        smart_results = super().query(nl_question, top_k)
        
        # Step 2: Generate natural language explanation
        if self.has_llm and smart_results.get('results'):
            nl_answer = self._generate_explanation(
                nl_question, 
                smart_results['query_type'],
                smart_results['results']
            )
        else:
            nl_answer = self._fallback_answer(
                nl_question,
                smart_results.get('results', [])
            )
        
        return {
            **smart_results,
            'nl_answer': nl_answer,
            'llm_enhanced': self.has_llm
        }
    
    def _generate_explanation(self, question: str, query_type: str, results: List[Dict]) -> str:
        """
        Generate natural language explanation grounded in Smart GraphRAG results.
        
        Uses structured prompting to avoid "I don't know" responses.
        """
        if not results:
            return "No matching tables found for your query."
        
        # Build rich context from results
        context = self._build_context(results[:3], query_type)
        
        # Structured prompt that forces specific output
        prompt = f"""You are a data catalog assistant explaining search results.

QUESTION: {question}

QUERY TYPE: {query_type}

SEARCH RESULTS (ranked by relevance):
{context}

INSTRUCTIONS:
1. Start with "Based on the search results..." or "The best match is..."
2. Name the top table and its key stats (row count, connections)
3. Explain WHY it matches (semantic similarity, relationships, etc.)
4. If relevant, mention 1-2 alternatives
5. Keep response to 2-3 sentences
6. ONLY use information from the search results above
7. Do NOT say "I don't know" - the results ARE the answer

RESPONSE:"""
        
        try:
            response = self.llm.invoke(prompt)
            answer = response.content.strip()
            
            # Validate response quality
            answer = self._validate_response(answer, results)
            return answer
            
        except Exception as e:
            print(f"⚠️ LLM generation failed: {e}")
            return self._fallback_answer(question, results)
    
    def _validate_response(self, answer: str, results: List[Dict]) -> str:
        """
        Validate LLM response and fallback if poor quality.
        """
        # Check for bad responses
        bad_indicators = [
            "i don't know",
            "i cannot",
            "no information",
            "not able to",
            "i'm sorry",
            "unfortunately",
            "i don't have",
            "cannot determine"
        ]
        
        answer_lower = answer.lower()
        
        # If bad response or too short, use template
        if any(phrase in answer_lower for phrase in bad_indicators):
            return self._template_answer(results)
        
        if len(answer) < 20:
            return self._template_answer(results)
        
        # If response doesn't mention any actual table names, it's hallucinating
        table_names = [r['table'].lower() for r in results]
        if not any(name in answer_lower for name in table_names):
            return self._template_answer(results)
        
        return answer
    
    def _build_context(self, top_results: List[Dict], query_type: str) -> str:
        """
        Build rich context from Smart GraphRAG results for LLM.
        """
        context_lines = []
        
        for i, result in enumerate(top_results, 1):
            # Basic info
            lines = [
                f"{i}. **{result['table']}**",
                f"   - Row count: {result['rows']:,}",
                f"   - Hybrid score: {result['score']:.1%}",
            ]
            
            # Query-type specific context
            if query_type == 'semantic_discovery':
                lines.append(f"   - Semantic similarity: {result.get('semantic_score', 0):.1f}%")
                lines.append(f"   - Graph centrality: {result.get('centrality', 0)} connections")
                if result.get('neighbors'):
                    neighbors = result['neighbors'][:3]
                    lines.append(f"   - Connected to: {', '.join(neighbors)}")
            
            elif query_type == 'duplicate_detection':
                lines.append(f"   - Marked as potential duplicate")
                if result.get('neighbors'):
                    lines.append(f"   - Duplicate of: {result['neighbors'][0]}")
                lines.append(f"   - Detection method: SANTOS algorithm (semantic + statistical)")
            
            elif query_type == 'relationship_traversal':
                reasoning = result.get('reasoning', 'connected via foreign key')
                lines.append(f"   - Relationship: {reasoning}")
                if result.get('neighbors'):
                    lines.append(f"   - Connected via: {', '.join(result['neighbors'][:2])}")
            
            elif query_type == 'metadata_filter':
                lines.append(f"   - Matched metadata criteria")
                if result.get('schema'):
                    lines.append(f"   - Schema: {result['schema']}")
            
            context_lines.append("\n".join(lines))
        
        return "\n\n".join(context_lines)
    
    def _fallback_answer(self, question: str, results: List[Dict]) -> str:
        """
        Template-based answer when LLM is unavailable.
        """
        if not results:
            return "No matching tables found in the catalog."
        
        return self._template_answer(results)
    
    def _template_answer(self, results: List[Dict]) -> str:
        """
        Generate reliable template-based answer.
        """
        if not results:
            return "No matching tables found."
        
        top = results[0]
        
        # Build informative answer
        answer_parts = [
            f"The best match is **{top['table']}**",
            f"with {top['rows']:,} rows",
            f"(relevance score: {top['score']:.1%})"
        ]
        
        # Add connection info if available
        if top.get('neighbors'):
            neighbors = top['neighbors'][:2]
            answer_parts.append(f"connected to {', '.join(neighbors)}")
        
        answer = " ".join(answer_parts) + "."
        
        # Add alternatives
        if len(results) > 1:
            alts = [r['table'] for r in results[1:3]]
            answer += f" Also consider: {', '.join(alts)}."
        
        return answer
    
    def get_system_info(self) -> Dict:
        """Return system status information."""
        base_info = {
            'engine': 'LLM-Enhanced Smart GraphRAG',
            'llm_available': self.has_llm,
            'llm_model': 'llama3.1' if self.has_llm else 'N/A',
            'temperature': self.temperature,
            'routing_accuracy': '60%',
            'routing_method': 'rule-based (4 routes)'
        }
        return base_info


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("🧪 TESTING LLM-ENHANCED SMART GRAPHRAG")
    print("="*70)
    print("Combines Smart routing (60%) + Ollama explanations")
    print("="*70)
    
    engine = LLMEnhancedSmartGraphRAG(temperature=0.3)
    
    print(f"\nSystem Info: {engine.get_system_info()}")
    
    test_queries = [
        "Which tables contain customer data?",
        "Show me all duplicate tables",
        "What tables connect to ORDERS?",
        "Which tables have more than 100,000 rows?",
        "Find tables related to payments",
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*70}")
        print(f"[{i}/{len(test_queries)}] Query: '{query}'")
        print("-"*70)
        
        result = engine.query(query, top_k=5)
        
        print(f"Query Type: {result['query_type']}")
        print(f"LLM Enhanced: {result['llm_enhanced']}")
        
        if result['results']:
            top = result['results'][0]
            print(f"Top Result: {top['table']} (score: {top['score']:.1%})")
        else:
            print("Top Result: None")
        
        print(f"\n💬 Natural Language Answer:")
        print(f"   {result['nl_answer']}")
    
    print(f"\n{'='*70}")
    print("✅ Testing Complete!")
    print("="*70)
    
    engine.close()