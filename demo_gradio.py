# demo_gradio.py

"""
NEXUS GraphRAG Demo - Professional UI v2.0
Beautiful, Clean, Modern Interface
Pranav Kharat - Northeastern University
"""

import gradio as gr
from src.graphrag.smart_graphrag_engine import SmartGraphRAGEngine
from src.graphrag.learned_graphrag_engine import LearnedGraphRAGEngine
from src.graphrag.llm_enhanced_smart_graphrag import LLMEnhancedSmartGraphRAG
# Updated import for Explainable GraphRAG support
from src.graphrag.unified_llm_graphrag import UnifiedLLMGraphRAG
from src.graphrag.query_features import QueryFeatureExtractor
from src.lineage.lineage_graph_builder import LineageGraphBuilder
from src.federation import FederatedKGBuilder, DatabricksMetadataExtractor, build_federated_graph
from neo4j import GraphDatabase
import json
import os

# ========================================
# INITIALIZE SYSTEMS
# ========================================

print("\n" + "="*60)
print("🚀 NEXUS GraphRAG - Initializing...")
print("="*60)

smart_engine = SmartGraphRAGEngine()
learned_engine = LearnedGraphRAGEngine()
feature_extractor = QueryFeatureExtractor()

try:
    enhanced_engine = LLMEnhancedSmartGraphRAG()
    has_enhanced = True
    print("✅ LLM-Enhanced Smart GraphRAG ready")
except Exception as e:
    has_enhanced = False
    print(f"⚠️ Enhanced engine unavailable: {e}")

try:
    unified_engine = UnifiedLLMGraphRAG()
    has_unified = True
    print("✅ Unified LLM GraphRAG ready")
except Exception as e:
    has_unified = False
    print(f"⚠️ Unified engine unavailable: {e}")

try:
    lineage_builder = LineageGraphBuilder()
    has_lineage = True
    print("✅ Lineage Explorer ready")
except Exception as e:
    has_lineage = False
    print(f"⚠️ Lineage unavailable: {e}")

# SHACL Validator
try:
    from src.governance.shacl_validator import SHACLValidator
    shacl_validator = SHACLValidator()
    has_shacl = True
    print("✅ SHACL Validator ready")
except Exception as e:
    has_shacl = False
    print(f"⚠️ SHACL unavailable: {e}")

neo4j_password = os.getenv('NEO4J_PASSWORD')
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", neo4j_password))

try:
    from src.federation.cross_source_duplicate_detector import CrossSourceDuplicateDetector
    cross_source_detector = CrossSourceDuplicateDetector()
    has_cross_source = True
    print("✅ Cross-Source Duplicate Detector ready")
except Exception as e:
    has_cross_source = False
    cross_source_detector = None
    print(f"⚠️ Cross-Source Detector unavailable: {e}")

try:
    with open('data/evaluation/comparative_results.json', 'r') as f:
        eval_results = json.load(f)
    has_eval_results = True
except:
    has_eval_results = False
    
try:
    from src.federation import FederatedKGBuilder, build_federated_graph
    federated_builder = FederatedKGBuilder()
    has_federation = True
    print("✅ Federated KG Builder ready")
except Exception as e:
    has_federation = False
    print(f"⚠️ Federation unavailable: {e}")    

print("="*60)
print("✅ All systems initialized!")
print("="*60 + "\n")

# ========================================
# CONFIGURATION
# ========================================

TABLE_CHOICES = [
    # Snowflake - Source
    "CUSTOMERS (Source)", "ORDERS (Source)", "PRODUCTS (Source)",
    "SELLERS (Source)", "GEOLOCATION (Source)", "ORDER_ITEMS (Source)",
    "ORDER_PAYMENTS (Source)", "ORDER_REVIEWS (Source)",
    # Snowflake - Marketing
    "CLIENT_DATA (Marketing)", "SALES_ORDERS (Marketing)", "PRODUCT_CATALOG (Marketing)",
    # Snowflake - Analytics
    "CUSTOMER_MASTER (Analytics)", "PURCHASE_HISTORY (Analytics)",
    # Databricks
    "sales_transactions (Databricks)", "customer_feedback (Databricks)",
]

SCHEMA_MAP = {
    # Snowflake tables
    'CUSTOMERS': 'OLIST_SALES', 'ORDERS': 'OLIST_SALES', 'PRODUCTS': 'OLIST_SALES',
    'SELLERS': 'OLIST_SALES', 'GEOLOCATION': 'OLIST_SALES', 'ORDER_ITEMS': 'OLIST_SALES',
    'ORDER_PAYMENTS': 'OLIST_SALES', 'ORDER_REVIEWS': 'OLIST_SALES',
    'CLIENT_DATA': 'OLIST_MARKETING', 'SALES_ORDERS': 'OLIST_MARKETING', 'PRODUCT_CATALOG': 'OLIST_MARKETING',
    'CUSTOMER_MASTER': 'OLIST_ANALYTICS', 'PURCHASE_HISTORY': 'OLIST_ANALYTICS',
    # Databricks tables
    'sales_transactions': 'databricks', 'customer_feedback': 'databricks',
}

# ========================================
# HELPER FUNCTIONS
# ========================================

def create_error_card(message):
    return f"""
<div style="background: linear-gradient(135deg, rgba(239, 68, 68, 0.15), rgba(220, 38, 38, 0.1)); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 16px; padding: 20px; display: flex; align-items: center; gap: 16px;">
    <div style="width: 48px; height: 48px; background: rgba(239, 68, 68, 0.2); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">⚠️</div>
    <div>
        <div style="font-size: 15px; font-weight: 600; color: #fca5a5; margin-bottom: 4px;">Error</div>
        <div style="font-size: 14px; color: #fecaca;">{message}</div>
    </div>
</div>
"""

def explain_cross_source_match(databricks_table: str, snowflake_table: str):
    """
    Generate detailed explanation for WHY two tables are similar.
    Called from the UI "Explain" button.
    """
    if not has_unified:
        return create_error_card("Unified engine not available")
    
    if not databricks_table or not snowflake_table:
        return """
<div style="background: rgba(251, 191, 36, 0.1); border: 1px solid rgba(251, 191, 36, 0.3); 
            border-radius: 12px; padding: 16px; color: #fbbf24;">
    ⚠️ Please enter both table names to explain the match.
</div>
"""
    
    try:
        # Call the new explain_match API
        explanation = unified_engine.explain_match(databricks_table.strip(), snowflake_table.strip())
        
        return f"""
<div style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.1), rgba(139, 92, 246, 0.1)); 
            border: 1px solid rgba(99, 102, 241, 0.3); border-radius: 16px; padding: 24px;">
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
        <div style="font-size: 32px;">🔍</div>
        <div>
            <div style="font-size: 16px; font-weight: 600; color: #a5b4fc;">Cross-Source Explanation</div>
            <div style="font-size: 13px; color: #a0a0b0;">
                {databricks_table} ↔ {snowflake_table}
            </div>
        </div>
    </div>
    
    <div style="background: rgba(20, 20, 30, 0.6); border-radius: 12px; padding: 16px; 
                font-size: 15px; line-height: 1.7; color: #e0e0e5;">
        {explanation}
    </div>
    
    <div style="margin-top: 16px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1);
                font-size: 12px; color: #6b7280;">
        💡 Powered by SANTOS algorithm + LLM explanation
    </div>
</div>
"""
    except Exception as e:
        return create_error_card(f"Explanation error: {str(e)}")

# ========================================
# UNIFIED SEARCH (IMPROVED UI)
# ========================================

def query_unified(question):
    """
    Unified search handling metadata, Databricks, cross-source, and sample data.
    """
    if not question.strip():
        return create_error_card("Please enter a question to search")
    
    if not has_unified:
        return create_error_card("Unified engine unavailable. Run: `ollama serve`")
    
    try:
        result = unified_engine.query(question, top_k=5)
        
        if not result['success']:
            return create_error_card(f"Query failed: {result.get('error', 'Unknown')}")
        
        intent = result['intent']
        
        # Intent styling based on type
        intent_styles = {
            'metadata': {
                'bg': "linear-gradient(135deg, rgba(139, 92, 246, 0.2), rgba(99, 102, 241, 0.1))",
                'border': "rgba(139, 92, 246, 0.4)",
                'icon': "📊",
                'label': "Metadata Query",
                'desc': "Searching Snowflake table catalog..."
            },
            'databricks': {
                'bg': "linear-gradient(135deg, rgba(249, 115, 22, 0.2), rgba(234, 88, 12, 0.1))",
                'border': "rgba(249, 115, 22, 0.4)",
                'icon': "🧱",
                'label': "Databricks Query",
                'desc': "Searching Databricks metadata..."
            },
            'cross_source': {
                'bg': "linear-gradient(135deg, rgba(236, 72, 153, 0.2), rgba(219, 39, 119, 0.1))",
                'border': "rgba(236, 72, 153, 0.4)",
                'icon': "🔀",
                'label': "Cross-Source Query",
                'desc': "Finding matches across platforms..."
            },
            'sample_data': {
                'bg': "linear-gradient(135deg, rgba(6, 182, 212, 0.2), rgba(59, 130, 246, 0.1))",
                'border': "rgba(6, 182, 212, 0.4)",
                'icon': "🔎",
                'label': "Sample Data Query",
                'desc': "Querying Neo4j sample data..."
            }
        }
        
        style = intent_styles.get(intent, intent_styles['metadata'])
        
        output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08); box-shadow: 0 20px 40px rgba(0,0,0,0.3);">
    
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #6366f1, #8b5cf6); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">🔍</div>
        <div style="flex: 1;">
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5; margin-bottom: 4px;">{question}</div>
            <div style="font-size: 13px; color: #606070;">Processed by Unified LLM GraphRAG</div>
        </div>
    </div>
    
    <div style="background: {style['bg']}; border: 1px solid {style['border']}; border-radius: 12px; padding: 16px; margin-bottom: 24px; display: flex; align-items: center; gap: 12px;">
        <span style="font-size: 28px;">{style['icon']}</span>
        <div>
            <div style="font-size: 15px; font-weight: 600; color: #f0f0f5;">{style['label']}</div>
            <div style="font-size: 13px; color: #a0a0b0;">{style['desc']}</div>
        </div>
    </div>
"""
        
        # DATABRICKS / CROSS-SOURCE / METADATA RESULTS (LangChain with Cypher)
        if intent in ['metadata', 'databricks', 'cross_source']:
            # Check if we have neo4j_results (LangChain path) or results (Smart path)
            neo4j_results = result.get('neo4j_results', [])
            smart_results = result.get('results', [])
            cypher = result.get('generated_cypher', '')
            
            # Use neo4j_results if available (LangChain), otherwise use smart_results
            if neo4j_results:
                # TABLE DISPLAY FOR LANGCHAIN RESULTS
                output += f"""
    <div style="margin-bottom: 24px;">
        <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 16px; display: flex; align-items: center; gap: 8px;">
            <span>📋</span> QUERY RESULTS
            <span style="background: rgba(99, 102, 241, 0.3); padding: 2px 10px; border-radius: 10px; font-size: 12px; color: #a5b4fc;">{len(neo4j_results)} rows</span>
        </div>
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; overflow: hidden; border: 1px solid rgba(255,255,255,0.06);">
"""
                if neo4j_results:
                    headers = list(neo4j_results[0].keys())[:5]  # Show up to 5 columns
                    output += '<div style="display: grid; grid-template-columns: repeat(' + str(len(headers)) + ', 1fr); background: rgba(99, 102, 241, 0.15); padding: 12px 16px;">'
                    for h in headers:
                        output += f'<div style="font-size: 11px; font-weight: 600; color: #a5b4fc; text-transform: uppercase;">{h}</div>'
                    output += "</div>"
                    
                    for row in neo4j_results[:15]:  # Show up to 15 rows
                        output += '<div style="display: grid; grid-template-columns: repeat(' + str(len(headers)) + ', 1fr); padding: 12px 16px; border-top: 1px solid rgba(255,255,255,0.04);">'
                        for h in headers:
                            val = str(row.get(h, ''))[:35]  # Truncate long values
                            output += f'<div style="font-size: 13px; color: #f0f0f5;">{val}</div>'
                        output += "</div>"
                    
                    if len(neo4j_results) > 15:
                        output += f'<div style="padding: 12px 16px; text-align: center; color: #606070; font-size: 12px;">... and {len(neo4j_results) - 15} more rows</div>'
                
                output += "</div></div>"
                
                # SHOW GENERATED CYPHER
                if cypher:
                    output += f"""
    <div style="margin-bottom: 24px;">
        <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 12px; display: flex; align-items: center; gap: 8px;">
            <span>⚡</span> GENERATED CYPHER
        </div>
        <div style="background: rgba(0, 0, 0, 0.4); border-radius: 12px; padding: 16px; border: 1px solid rgba(255,255,255,0.06);">
            <code style="font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #a5b4fc; white-space: pre-wrap;">{cypher}</code>
        </div>
    </div>
"""
            
            elif smart_results:
                # CARD DISPLAY FOR SMART GRAPHRAG RESULTS (existing code)
                output += """
    <div style="margin-bottom: 24px;">
        <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 16px; display: flex; align-items: center; gap: 8px;">
            <span>📋</span> RESULTS
        </div>
        <div style="display: grid; gap: 12px;">
"""
                for i, r in enumerate(smart_results[:5], 1):
                    score = r.get('score', r.get('final_score', 0))
                    is_top = i == 1
                    
                    # Source-aware styling
                    source = r.get('source', r.get('schema', 'snowflake'))
                    if 'databricks' in str(source).lower():
                        source_icon = "🧱"
                        source_color = "#fdba74"
                    elif 'cross' in str(r.get('reasoning', '')).lower():
                        source_icon = "🔀"
                        source_color = "#f9a8d4"
                    else:
                        source_icon = "❄️"
                        source_color = "#93c5fd"
                    
                    card_bg = "linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.08))" if is_top else "rgba(30, 30, 45, 0.9)"
                    card_border = "rgba(16, 185, 129, 0.4)" if is_top else "rgba(255,255,255,0.06)"
                    rank_bg = "#10b981" if is_top else "#6366f1"
                    
                    table_name = r.get('table', r.get('column_name', 'Unknown'))
                    rows = r.get('rows', 0)
                    reasoning = r.get('reasoning', '')
                    centrality = r.get('centrality', 0)
                    neighbors = r.get('neighbors', [])
                    
                    output += f"""
            <div style="background: {card_bg}; border: 1px solid {card_border}; border-radius: 12px; padding: 16px;">
                <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 12px;">
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span style="background: {rank_bg}; color: white; width: 24px; height: 24px; border-radius: 6px; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 700;">#{i}</span>
                        <span style="color: {source_color}; font-size: 16px;">{source_icon}</span>
                        <span style="font-size: 14px; font-weight: 600; color: #f0f0f5;">{table_name}</span>
                    </div>
                    <span style="font-size: 15px; color: #10b981; font-weight: 600;">{score:.1f}%</span>
                </div>
                <div style="display: flex; gap: 16px; font-size: 12px; color: #a0a0b0; margin-bottom: 8px;">
                    <span>📊 {rows:,} rows</span>
                    {f'<span>🔗 {centrality} connections</span>' if centrality > 0 else ''}
                </div>
                {f'<div style="font-size: 11px; color: #606070; background: rgba(0,0,0,0.2); padding: 8px 12px; border-radius: 6px;">{reasoning}</div>' if reasoning else ''}
            </div>
"""
                output += "</div></div>"
            
            else:
                # No results
                output += """
    <div style="text-align: center; padding: 32px; color: #606070;">
        <div style="font-size: 48px; margin-bottom: 16px;">🔍</div>
        <div style="font-size: 14px;">No matching results found</div>
    </div>
"""
        
        # SAMPLE DATA RESULTS
        elif intent == 'sample_data' and 'neo4j_results' in result:
            neo4j_results = result['neo4j_results']
            cypher = result.get('generated_cypher', '')
            
            if neo4j_results:
                # Count query - big number display
                if len(neo4j_results) == 1 and 'count' in neo4j_results[0]:
                    count_value = neo4j_results[0]['count']
                    output += f"""
    <div style="margin-bottom: 24px;">
        <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 16px; display: flex; align-items: center; gap: 8px;">
            <span>📊</span> QUERY RESULT
        </div>
        <div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(5, 150, 105, 0.1)); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 16px; padding: 40px; text-align: center;">
            <div style="font-size: 64px; font-weight: 800; color: #6ee7b7; margin-bottom: 8px;">{count_value:,}</div>
            <div style="font-size: 18px; color: #a0a0b0;">records found</div>
        </div>
    </div>
"""
                else:
                    # Table display
                    output += f"""
    <div style="margin-bottom: 24px;">
        <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 16px; display: flex; align-items: center; gap: 8px;">
            <span>💾</span> DATA RESULTS
            <span style="background: rgba(99, 102, 241, 0.3); padding: 2px 10px; border-radius: 10px; font-size: 12px; color: #a5b4fc;">{len(neo4j_results)} rows</span>
        </div>
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; overflow: hidden; border: 1px solid rgba(255,255,255,0.06);">
"""
                    if neo4j_results:
                        headers = list(neo4j_results[0].keys())[:4]
                        output += '<div style="display: grid; grid-template-columns: repeat(' + str(len(headers)) + ', 1fr); background: rgba(99, 102, 241, 0.15); padding: 12px 16px;">'
                        for h in headers:
                            output += f'<div style="font-size: 11px; font-weight: 600; color: #a5b4fc; text-transform: uppercase;">{h}</div>'
                        output += "</div>"
                        
                        for row in neo4j_results[:5]:
                            output += '<div style="display: grid; grid-template-columns: repeat(' + str(len(headers)) + ', 1fr); padding: 12px 16px; border-top: 1px solid rgba(255,255,255,0.04);">'
                            for h in headers:
                                val = str(row.get(h, ''))[:25]
                                output += f'<div style="font-size: 13px; color: #f0f0f5;">{val}</div>'
                            output += "</div>"
                    output += "</div></div>"
            
            # Cypher Query Display
            if cypher:
                output += f"""
    <div style="margin-bottom: 24px;">
        <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 12px; display: flex; align-items: center; gap: 8px;">
            <span>⚡</span> GENERATED CYPHER
        </div>
        <div style="background: rgba(0, 0, 0, 0.4); border-radius: 12px; padding: 16px; border: 1px solid rgba(255,255,255,0.06);">
            <code style="font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #a5b4fc; white-space: pre-wrap;">{cypher}</code>
        </div>
    </div>
"""
        
        # AI ANSWER
        nl_answer = result.get('nl_answer', 'No answer generated')
        confidence = result.get('confidence', 'medium')
        
        conf_colors = {
            'high': ('#6ee7b7', 'rgba(16, 185, 129, 0.2)'),
            'medium': ('#fcd34d', 'rgba(245, 158, 11, 0.2)'),
            'low': ('#fca5a5', 'rgba(239, 68, 68, 0.2)')
        }
        conf_color, conf_bg = conf_colors.get(confidence, conf_colors['medium'])
        
        output += f"""
    <div style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.1), rgba(139, 92, 246, 0.05)); border-radius: 16px; padding: 24px; border-left: 4px solid #6366f1;">
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px;">
            <div style="display: flex; align-items: center; gap: 10px;">
                <div style="width: 32px; height: 32px; background: linear-gradient(135deg, #6366f1, #8b5cf6); border-radius: 8px; display: flex; align-items: center; justify-content: center;">💬</div>
                <span style="font-size: 14px; font-weight: 600; color: #a5b4fc;">AI Answer</span>
            </div>
            <span style="background: {conf_bg}; color: {conf_color}; padding: 4px 12px; border-radius: 8px; font-size: 11px; font-weight: 600;">{confidence.upper()} CONFIDENCE</span>
        </div>
        <div style="font-size: 16px; line-height: 1.7; color: #f0f0f5;">{nl_answer}</div>
    </div>
</div>
"""
        return output
        
    except Exception as e:
        return create_error_card(f"Error: {str(e)}")
# ========================================
# LINEAGE HANDLERS
# ========================================

def explore_lineage(table_selection, direction):
    if not table_selection:
        return create_error_card("Please select a table")
    if not has_lineage:
        return create_error_card("Lineage unavailable")
    
    # Parse table name - handle both formats
    table_name = table_selection.split(" (")[0].strip()
    
    # Check if it's Databricks or Snowflake
    is_databricks = "(Databricks)" in table_selection
    
    if is_databricks:
        schema = 'databricks'
        # Keep original case for Databricks
    else:
        table_name = table_name.upper()
        schema = SCHEMA_MAP.get(table_name)
        if not schema:
            return create_error_card(f"Unknown table: {table_name}")
    
    try:
        if direction == "Upstream (Sources)":
            lineage = lineage_builder.get_upstream_lineage(schema, table_name)
            icon, label, arrow = "⬆️", "Sources (derives from)", "←"
        else:
            lineage = lineage_builder.get_downstream_lineage(schema, table_name)
            icon, label, arrow = "⬇️", "Targets (feeds into)", "→"
        
        # Platform badge
        if is_databricks:
            platform_badge = '<span style="background: rgba(249, 115, 22, 0.3); padding: 4px 12px; border-radius: 8px; font-size: 12px; color: #fdba74;">🧱 DATABRICKS</span>'
            header_bg = "linear-gradient(135deg, #f97316, #ea580c)"
        else:
            platform_badge = '<span style="background: rgba(59, 130, 246, 0.3); padding: 4px 12px; border-radius: 8px; font-size: 12px; color: #93c5fd;">❄️ SNOWFLAKE</span>'
            header_bg = "linear-gradient(135deg, #8b5cf6, #6366f1)"
        
        output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 48px; height: 48px; background: {header_bg}; border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">{icon}</div>
        <div style="flex: 1;">
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 4px;">
                <span style="font-size: 18px; font-weight: 600; color: #f0f0f5;">{table_name}</span>
                {platform_badge}
            </div>
            <div style="font-size: 13px; color: #a0a0b0;">{label}</div>
        </div>
    </div>
"""
        
        if not lineage:
            output += f"""
    <div style="text-align: center; padding: 48px; color: #606070;">
        <div style="font-size: 48px; margin-bottom: 16px;">🔭</div>
        <div style="font-size: 16px; color: #a0a0b0;">No {label.lower()} lineage found</div>
    </div>
"""
        else:
            output += '<div style="display: grid; gap: 12px;">'
            for item in lineage:
                conf = item.get('confidence', 0) or 0
                source_type = item.get('source_type', 'snowflake')
                
                # Styling based on confidence
                border = "#10b981" if conf >= 0.9 else "#f59e0b" if conf >= 0.7 else "#6b7280"
                
                # Platform icon
                if source_type == 'databricks':
                    platform_icon = "🧱"
                    platform_color = "#fdba74"
                else:
                    platform_icon = "❄️"
                    platform_color = "#93c5fd"
                
                output += f"""
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 16px; border-left: 4px solid {border};">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span style="font-size: 15px; font-weight: 600; color: #f0f0f5;">{arrow} {item['schema']}.{item['table']}</span>
                    <span style="color: {platform_color};">{platform_icon}</span>
                </div>
                <span style="background: #6366f1; color: white; padding: 4px 10px; border-radius: 8px; font-size: 11px;">{item.get('lineage_type', 'UNKNOWN')}</span>
            </div>
            <div style="display: flex; gap: 16px; font-size: 12px; color: #a0a0b0;">
                <span>📊 {item.get('rows', 0):,} rows</span>
                <span>🎯 {conf:.0%} confidence</span>
                <span>📍 {source_type}</span>
            </div>
        </div>
"""
            output += '</div>'
        
        output += '</div>'
        return output
    except Exception as e:
        return create_error_card(f"Error: {str(e)}")

def show_full_lineage():
    if not has_lineage:
        return create_error_card("Lineage unavailable")
    try:
        graph = lineage_builder.get_full_lineage_graph()
        stats = lineage_builder.get_lineage_statistics()
        db_summary = lineage_builder.get_databricks_lineage_summary()
        
        # Count by platform
        snowflake_edges = len([e for e in graph['edges'] if e.get('platform') == 'snowflake'])
        databricks_edges = len([e for e in graph['edges'] if e.get('platform') == 'databricks'])
        
        # Build detailed edge list for Snowflake
        snowflake_edge_html = ""
        for edge in graph['edges']:
            if edge.get('platform') == 'snowflake':
                conf = edge.get('confidence', 0) or 0
                conf_color = "#10b981" if conf >= 0.9 else "#f59e0b" if conf >= 0.7 else "#6b7280"
                snowflake_edge_html += f"""
                <div style="display: flex; align-items: center; justify-content: space-between; padding: 10px 16px; background: rgba(0,0,0,0.2); border-radius: 8px; margin-bottom: 8px;">
                    <div style="display: flex; align-items: center; gap: 12px;">
                        <span style="color: #6ee7b7; font-weight: 500; font-size: 13px;">{edge['target'].split('.')[-1]}</span>
                        <span style="color: #606070;">→</span>
                        <span style="color: #93c5fd; font-weight: 500; font-size: 13px;">{edge['source'].split('.')[-1]}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 12px;">
                        <span style="background: rgba(99, 102, 241, 0.3); padding: 2px 8px; border-radius: 6px; font-size: 11px; color: #a5b4fc;">{edge['type']}</span>
                        <span style="color: {conf_color}; font-size: 12px; font-weight: 600;">{conf:.0%}</span>
                    </div>
                </div>
                """
        
        return f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #8b5cf6, #6366f1); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">🗺️</div>
        <div>
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5;">Complete Data Lineage Map</div>
            <div style="font-size: 13px; color: #a0a0b0;">Tracking data flow across Snowflake & Databricks</div>
        </div>
    </div>
    
    <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 24px;">
        <div style="background: rgba(99, 102, 241, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(99, 102, 241, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #a5b4fc;">{graph['node_count']}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Tables</div>
        </div>
        <div style="background: rgba(16, 185, 129, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(16, 185, 129, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #6ee7b7;">{graph['edge_count']}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Lineage Edges</div>
        </div>
        <div style="background: rgba(59, 130, 246, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(59, 130, 246, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #93c5fd;">{snowflake_edges}</div>
            <div style="font-size: 12px; color: #a0a0b0;">❄️ Snowflake</div>
        </div>
        <div style="background: rgba(249, 115, 22, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(249, 115, 22, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #fdba74;">{databricks_edges}</div>
            <div style="font-size: 12px; color: #a0a0b0;">🧱 Databricks</div>
        </div>
        <div style="background: rgba(139, 92, 246, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(139, 92, 246, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #c4b5fd;">{stats.get('avg_confidence', 0):.0%}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Avg Confidence</div>
        </div>
    </div>
    
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
    
        <div style="background: rgba(30, 30, 45, 0.5); border-radius: 16px; padding: 24px; border: 1px solid rgba(59, 130, 246, 0.2);">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px;">
                <span style="font-size: 20px;">❄️</span>
                <span style="font-size: 16px; font-weight: 600; color: #93c5fd;">Snowflake Lineage</span>
                <span style="background: rgba(59, 130, 246, 0.3); padding: 2px 10px; border-radius: 10px; font-size: 11px; color: #93c5fd;">{snowflake_edges} edges</span>
            </div>
            
            <div style="background: rgba(0,0,0,0.2); border-radius: 12px; padding: 16px; margin-bottom: 16px;">
                <div style="text-align: center; font-size: 11px; color: #10b981; margin-bottom: 12px; font-weight: 600;">📦 SOURCE TABLES (OLIST_SALES)</div>
                <div style="display: flex; justify-content: center; gap: 8px; flex-wrap: wrap; margin-bottom: 16px;">
                    <div style="background: linear-gradient(135deg, #10b981, #059669); padding: 8px 14px; border-radius: 6px; color: white; font-weight: 600; font-size: 12px;">CUSTOMERS</div>
                    <div style="background: linear-gradient(135deg, #10b981, #059669); padding: 8px 14px; border-radius: 6px; color: white; font-weight: 600; font-size: 12px;">ORDERS</div>
                    <div style="background: linear-gradient(135deg, #10b981, #059669); padding: 8px 14px; border-radius: 6px; color: white; font-weight: 600; font-size: 12px;">PRODUCTS</div>
                </div>
                <div style="text-align: center; font-size: 18px; color: #404050; margin: 8px 0;">↓</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                    <div>
                        <div style="text-align: center; font-size: 10px; color: #f59e0b; margin-bottom: 6px; font-weight: 600;">MARKETING</div>
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <div style="background: rgba(245, 158, 11, 0.2); padding: 6px; border-radius: 4px; color: #fcd34d; text-align: center; font-size: 11px;">CLIENT_DATA</div>
                            <div style="background: rgba(245, 158, 11, 0.2); padding: 6px; border-radius: 4px; color: #fcd34d; text-align: center; font-size: 11px;">SALES_ORDERS</div>
                            <div style="background: rgba(245, 158, 11, 0.2); padding: 6px; border-radius: 4px; color: #fcd34d; text-align: center; font-size: 11px;">PRODUCT_CATALOG</div>
                        </div>
                    </div>
                    <div>
                        <div style="text-align: center; font-size: 10px; color: #8b5cf6; margin-bottom: 6px; font-weight: 600;">ANALYTICS</div>
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <div style="background: rgba(139, 92, 246, 0.2); padding: 6px; border-radius: 4px; color: #c4b5fd; text-align: center; font-size: 11px;">CUSTOMER_MASTER</div>
                            <div style="background: rgba(139, 92, 246, 0.2); padding: 6px; border-radius: 4px; color: #c4b5fd; text-align: center; font-size: 11px;">PURCHASE_HISTORY</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div style="font-size: 12px; color: #a0a0b0; margin-bottom: 10px; font-weight: 600;">📋 Edge Details</div>
            {snowflake_edge_html}
        </div>
        
        <div style="background: rgba(30, 30, 45, 0.5); border-radius: 16px; padding: 24px; border: 1px solid rgba(249, 115, 22, 0.2);">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px;">
                <span style="font-size: 20px;">🧱</span>
                <span style="font-size: 16px; font-weight: 600; color: #fdba74;">Databricks Lineage</span>
                <span style="background: rgba(249, 115, 22, 0.3); padding: 2px 10px; border-radius: 10px; font-size: 11px; color: #fdba74;">{databricks_edges} edge</span>
            </div>
            
            <div style="background: rgba(0,0,0,0.2); border-radius: 12px; padding: 20px; margin-bottom: 16px;">
                <div style="text-align: center; font-size: 11px; color: #a0a0b0; margin-bottom: 8px;">workspace.sample_data</div>
                
                <div style="display: flex; justify-content: center; align-items: center; gap: 16px; margin: 20px 0;">
                    <div style="text-align: center;">
                        <div style="background: linear-gradient(135deg, #f97316, #ea580c); padding: 16px 20px; border-radius: 10px; color: white; font-weight: 700; font-size: 13px; margin-bottom: 8px;">sales_transactions</div>
                        <div style="font-size: 11px; color: #a0a0b0;">150 rows • 13 cols</div>
                        <div style="font-size: 10px; color: #fdba74;">Sales Team</div>
                    </div>
                    
                    <div style="display: flex; flex-direction: column; align-items: center; padding: 0 8px;">
                        <div style="font-size: 24px; color: #6ee7b7;">←</div>
                        <div style="background: rgba(16, 185, 129, 0.2); padding: 4px 10px; border-radius: 6px; font-size: 10px; color: #6ee7b7; font-weight: 600;">FOREIGN_KEY</div>
                        <div style="font-size: 10px; color: #a0a0b0; margin-top: 4px;">transaction_id</div>
                    </div>
                    
                    <div style="text-align: center;">
                        <div style="background: linear-gradient(135deg, #fb923c, #f97316); padding: 16px 20px; border-radius: 10px; color: white; font-weight: 700; font-size: 13px; margin-bottom: 8px;">customer_feedback</div>
                        <div style="font-size: 11px; color: #a0a0b0;">100 rows • 12 cols</div>
                        <div style="font-size: 10px; color: #fdba74;">CX Team</div>
                    </div>
                </div>
            </div>
            
            <div style="font-size: 12px; color: #a0a0b0; margin-bottom: 10px; font-weight: 600;">📋 Edge Details</div>
            <div style="display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: rgba(0,0,0,0.2); border-radius: 8px;">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <span style="color: #fdba74; font-weight: 500; font-size: 13px;">customer_feedback</span>
                    <span style="color: #606070;">→</span>
                    <span style="color: #fb923c; font-weight: 500; font-size: 13px;">sales_transactions</span>
                </div>
                <div style="display: flex; align-items: center; gap: 12px;">
                    <span style="background: rgba(16, 185, 129, 0.3); padding: 2px 8px; border-radius: 6px; font-size: 11px; color: #6ee7b7;">FOREIGN_KEY</span>
                    <span style="color: #10b981; font-size: 12px; font-weight: 600;">100%</span>
                </div>
            </div>
            
            <div style="margin-top: 16px; padding: 12px; background: rgba(249, 115, 22, 0.1); border-radius: 8px; border: 1px solid rgba(249, 115, 22, 0.2);">
                <div style="font-size: 11px; color: #fdba74; font-weight: 600; margin-bottom: 8px;">🔑 Join Information</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 12px;">
                    <div><span style="color: #a0a0b0;">Column:</span> <span style="color: #f0f0f5;">transaction_id</span></div>
                    <div><span style="color: #a0a0b0;">Confidence:</span> <span style="color: #6ee7b7;">100%</span></div>
                    <div><span style="color: #a0a0b0;">Type:</span> <span style="color: #f0f0f5;">Foreign Key</span></div>
                    <div><span style="color: #a0a0b0;">Direction:</span> <span style="color: #f0f0f5;">Many-to-One</span></div>
                </div>
            </div>
        </div>
    </div>
    
    <div style="margin-top: 20px; padding: 16px; background: rgba(30, 30, 45, 0.5); border-radius: 12px; display: flex; justify-content: space-around; align-items: center;">
        <div style="text-align: center;">
            <div style="font-size: 11px; color: #a0a0b0; margin-bottom: 4px;">Lineage Types</div>
            <div style="display: flex; gap: 8px;">
                {"".join([f'<span style="background: rgba(99, 102, 241, 0.2); padding: 4px 10px; border-radius: 6px; font-size: 11px; color: #a5b4fc;">{t}</span>' for t in stats.get('lineage_types', [])])}
            </div>
        </div>
        <div style="height: 30px; width: 1px; background: rgba(255,255,255,0.1);"></div>
        <div style="text-align: center;">
            <div style="font-size: 11px; color: #a0a0b0; margin-bottom: 4px;">Data Sources</div>
            <div style="display: flex; gap: 8px;">
                <span style="background: rgba(59, 130, 246, 0.2); padding: 4px 10px; border-radius: 6px; font-size: 11px; color: #93c5fd;">❄️ Snowflake</span>
                <span style="background: rgba(249, 115, 22, 0.2); padding: 4px 10px; border-radius: 6px; font-size: 11px; color: #fdba74;">🧱 Databricks</span>
            </div>
        </div>
        <div style="height: 30px; width: 1px; background: rgba(255,255,255,0.1);"></div>
        <div style="text-align: center;">
            <div style="font-size: 11px; color: #a0a0b0; margin-bottom: 4px;">RQ2 Result</div>
            <div style="font-size: 14px; font-weight: 700; color: #10b981;">100% F1 Score</div>
        </div>
    </div>
</div>
"""
    except Exception as e:
        return create_error_card(f"Error: {str(e)}")

# ========================================
# OTHER HANDLERS
# ========================================

def compare_engines(question):
    if not question.strip():
        return create_error_card("Please enter a question")
    
    smart = smart_engine.query(question, top_k=5)
    learned = learned_engine.query(question, top_k=5)
    
    output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="text-align: center; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="font-size: 16px; color: #f0f0f5;">"{question}"</div>
    </div>
    
    <div style="display: grid; grid-template-columns: 1fr auto 1fr; gap: 20px; align-items: start;">
        <div style="background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 16px; padding: 20px;">
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
                <span style="font-size: 24px;">🔬</span>
                <div>
                    <div style="font-size: 15px; font-weight: 600; color: #6ee7b7;">Smart GraphRAG</div>
                    <div style="font-size: 12px; color: #a0a0b0;">Rule-based • 60%</div>
                </div>
            </div>
            <div style="font-size: 12px; color: #a0a0b0; margin-bottom: 12px;">Route: <code style="color: #6ee7b7;">{smart['query_type']}</code></div>
            <div style="display: grid; gap: 8px;">
"""
    for i, r in enumerate(smart['results'][:3], 1):
        output += f'<div style="background: rgba(0,0,0,0.2); padding: 8px 12px; border-radius: 6px; font-size: 12px; color: #f0f0f5;">#{i} {r.get("table", "N/A")}</div>'
    
    output += f"""
            </div>
        </div>
        
        <div style="font-size: 24px; font-weight: 700; color: #606070; align-self: center;">VS</div>
        
        <div style="background: rgba(139, 92, 246, 0.1); border: 1px solid rgba(139, 92, 246, 0.3); border-radius: 16px; padding: 20px;">
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
                <span style="font-size: 24px;">🤖</span>
                <div>
                    <div style="font-size: 15px; font-weight: 600; color: #c4b5fd;">Learned GraphRAG</div>
                    <div style="font-size: 12px; color: #a0a0b0;">XGBoost • 53.3%</div>
                </div>
            </div>
            <div style="font-size: 12px; color: #a0a0b0; margin-bottom: 12px;">Route: <code style="color: #c4b5fd;">{learned['query_type']}</code></div>
            <div style="display: grid; gap: 8px;">
"""
    for i, r in enumerate(learned['results'][:3], 1):
        output += f'<div style="background: rgba(0,0,0,0.2); padding: 8px 12px; border-radius: 6px; font-size: 12px; color: #f0f0f5;">#{i} {r.get("table", "N/A")}</div>'
    
    output += "</div></div></div></div>"
    return output

def show_duplicates(scope="All"):
    """Show duplicates with optional scope filtering"""
    
    # 1. Get WITHIN-SNOWFLAKE duplicates (existing OLIST_DUPLICATE)
    with neo4j_driver.session() as session:
        sf_result = session.run("""
            MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData)
            RETURN t1.schema + '.' + t1.name as source, 
                   t2.schema + '.' + t2.name as target,
                   d.confidence as confidence, 
                   d.match_type as type
            ORDER BY d.confidence DESC
        """)
        snowflake_duplicates = list(sf_result)
        
        # 2. Get CROSS-SOURCE duplicates (SIMILAR_TO edges)
        cs_result = session.run("""
            MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
            RETURN db.full_name as databricks_table,
                   sf.schema + '.' + sf.name as snowflake_table,
                   r.score as score,
                   r.confidence as confidence,
                   r.semantic_score as semantic,
                   r.matching_columns as columns
            ORDER BY r.score DESC
        """)
        cross_source_duplicates = list(cs_result)
    
    # Filter based on scope
    show_snowflake = scope in ["All", "Within Snowflake"]
    show_cross_source = scope in ["All", "Cross-Source (Databricks↔Snowflake)"]
    
    # Build output
    output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #f59e0b, #d97706); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">🔄</div>
        <div style="flex: 1;">
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5;">SANTOS Duplicate Detection</div>
            <div style="font-size: 13px; color: #a0a0b0;">Within-source & cross-source semantic similarity</div>
        </div>
    </div>
    
    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 24px;">
        <div style="background: rgba(59, 130, 246, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(59, 130, 246, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #93c5fd;">{len(snowflake_duplicates)}</div>
            <div style="font-size: 12px; color: #a0a0b0;">❄️ Within Snowflake</div>
        </div>
        <div style="background: rgba(249, 115, 22, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(249, 115, 22, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #fdba74;">{len(cross_source_duplicates)}</div>
            <div style="font-size: 12px; color: #a0a0b0;">🔀 Cross-Source</div>
        </div>
        <div style="background: rgba(16, 185, 129, 0.15); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid rgba(16, 185, 129, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #6ee7b7;">{len(snowflake_duplicates) + len(cross_source_duplicates)}</div>
            <div style="font-size: 12px; color: #a0a0b0;">📊 Total Pairs</div>
        </div>
    </div>
    
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
"""
    
    # SNOWFLAKE DUPLICATES
    if show_snowflake:
        output += f"""
        <div style="background: rgba(30, 30, 45, 0.5); border-radius: 16px; padding: 20px; border: 1px solid rgba(59, 130, 246, 0.2);">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">
                <span style="font-size: 20px;">❄️</span>
                <span style="font-size: 15px; font-weight: 600; color: #93c5fd;">Within Snowflake</span>
                <span style="background: rgba(59, 130, 246, 0.3); padding: 2px 10px; border-radius: 10px; font-size: 11px; color: #93c5fd;">{len(snowflake_duplicates)} pairs</span>
            </div>
            <div style="display: grid; gap: 10px; max-height: 400px; overflow-y: auto;">
"""
        if snowflake_duplicates:
            for dup in snowflake_duplicates:
                conf = dup['confidence'] or 0
                border = "#10b981" if dup['type'] == 'EXACT_SCHEMA' else "#f59e0b"
                source_name = dup['source'].split('.')[-1]
                target_name = dup['target'].split('.')[-1]
                
                output += f"""
                <div style="background: rgba(0,0,0,0.2); border-radius: 10px; padding: 14px; border-left: 4px solid {border};">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <span style="font-size: 16px; font-weight: 700; color: #10b981;">{conf*100:.0f}%</span>
                        <span style="background: rgba(99, 102, 241, 0.2); padding: 3px 8px; border-radius: 6px; font-size: 10px; color: #a5b4fc;">{dup['type'].replace('_', ' ')}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px; font-size: 13px;">
                        <span style="color: #6ee7b7; font-weight: 500;">{source_name}</span>
                        <span style="color: #404050;">⟷</span>
                        <span style="color: #a5b4fc; font-weight: 500;">{target_name}</span>
                    </div>
                    <div style="font-size: 11px; color: #606070; margin-top: 6px;">
                        {dup['source'].split('.')[0]} → {dup['target'].split('.')[0]}
                    </div>
                </div>
"""
        else:
            output += """
                <div style="text-align: center; padding: 32px; color: #606070;">
                    <div style="font-size: 32px; margin-bottom: 8px;">📭</div>
                    <div style="font-size: 13px;">No within-Snowflake duplicates</div>
                </div>
"""
        output += "</div></div>"

    # CROSS-SOURCE DUPLICATES
    if show_cross_source:
        output += f"""
        <div style="background: rgba(30, 30, 45, 0.5); border-radius: 16px; padding: 20px; border: 1px solid rgba(249, 115, 22, 0.2);">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">
                <span style="font-size: 20px;">🔀</span>
                <span style="font-size: 15px; font-weight: 600; color: #fdba74;">Cross-Source</span>
                <span style="background: rgba(249, 115, 22, 0.3); padding: 2px 10px; border-radius: 10px; font-size: 11px; color: #fdba74;">{len(cross_source_duplicates)} pairs</span>
            </div>
            <div style="display: grid; gap: 10px; max-height: 400px; overflow-y: auto;">
"""
        if cross_source_duplicates:
            for cs in cross_source_duplicates:
                score = cs['score'] or 0
                conf = cs['confidence'] or 'low'
                
                if conf == 'high':
                    border = "#10b981"
                elif conf == 'medium':
                    border = "#f59e0b"
                else:
                    border = "#6b7280"
                
                db_name = cs['databricks_table'].split('.')[-1]
                sf_name = cs['snowflake_table'].split('.')[-1]
                
                output += f"""
                <div style="background: rgba(0,0,0,0.2); border-radius: 10px; padding: 14px; border-left: 4px solid {border};">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <span style="font-size: 16px; font-weight: 700; color: #fcd34d;">{score:.1%}</span>
                        <span style="background: rgba(99, 102, 241, 0.2); padding: 3px 8px; border-radius: 6px; font-size: 10px; color: #a5b4fc; text-transform: uppercase;">{conf}</span>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr auto 1fr; gap: 8px; align-items: center;">
                        <div style="text-align: center;">
                            <div style="font-size: 10px; color: #fdba74;">🧱 Databricks</div>
                            <div style="font-size: 12px; color: #f0f0f5; font-weight: 500;">{db_name}</div>
                        </div>
                        <span style="color: #404050;">↔</span>
                        <div style="text-align: center;">
                            <div style="font-size: 10px; color: #93c5fd;">❄️ Snowflake</div>
                            <div style="font-size: 12px; color: #f0f0f5; font-weight: 500;">{sf_name}</div>
                        </div>
                    </div>
                </div>
"""
        else:
            output += """
                <div style="text-align: center; padding: 32px; color: #606070;">
                    <div style="font-size: 32px; margin-bottom: 8px;">🔍</div>
                    <div style="font-size: 13px;">No cross-source matches yet</div>
                    <div style="font-size: 11px; margin-top: 4px;">Run detection below</div>
                </div>
"""
        output += "</div></div>"
    
    output += """
    </div>
</div>
"""
    return output


def run_cross_source_detection(threshold: float = 0.25):
    """Run SANTOS cross-source detection"""
    if not has_cross_source:
        return create_error_card("Cross-Source Detector unavailable. Check imports."), 0
    
    try:
        results = cross_source_detector.detect_cross_source_duplicates(min_threshold=threshold)
        
        if not results:
            return f"""
<div style="background: rgba(245, 158, 11, 0.1); border-radius: 12px; padding: 20px; border: 1px solid rgba(245, 158, 11, 0.3); text-align: center;">
    <div style="font-size: 32px; margin-bottom: 8px;">🔍</div>
    <div style="font-size: 14px; color: #fcd34d;">No matches found above {threshold:.0%} threshold</div>
    <div style="font-size: 12px; color: #a0a0b0; margin-top: 4px;">Try lowering the threshold</div>
</div>
""", 0
        
        # Count by confidence
        high = len([r for r in results if r.confidence == 'high'])
        medium = len([r for r in results if r.confidence == 'medium'])
        low = len([r for r in results if r.confidence == 'low'])
        
        output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 16px; padding: 24px; border: 1px solid rgba(255,255,255,0.08);">
    
    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px;">
        <div style="display: flex; align-items: center; gap: 12px;">
            <span style="font-size: 24px;">✅</span>
            <div>
                <div style="font-size: 16px; font-weight: 600; color: #6ee7b7;">Detection Complete!</div>
                <div style="font-size: 12px; color: #a0a0b0;">{len(results)} matches at {threshold:.0%} threshold</div>
            </div>
        </div>
        <div style="display: flex; gap: 12px;">
            <span style="background: rgba(16, 185, 129, 0.2); padding: 4px 12px; border-radius: 8px; font-size: 12px; color: #6ee7b7;">🟢 {high} high</span>
            <span style="background: rgba(245, 158, 11, 0.2); padding: 4px 12px; border-radius: 8px; font-size: 12px; color: #fcd34d;">🟡 {medium} med</span>
            <span style="background: rgba(107, 114, 128, 0.2); padding: 4px 12px; border-radius: 8px; font-size: 12px; color: #9ca3af;">🔴 {low} low</span>
        </div>
    </div>
    
    <div style="display: grid; gap: 12px; max-height: 400px; overflow-y: auto;">
"""
        
        for score in results[:10]:  # Top 10
            if score.confidence == 'high':
                conf_color, conf_bg = "#6ee7b7", "rgba(16, 185, 129, 0.15)"
            elif score.confidence == 'medium':
                conf_color, conf_bg = "#fcd34d", "rgba(245, 158, 11, 0.15)"
            else:
                conf_color, conf_bg = "#9ca3af", "rgba(107, 114, 128, 0.15)"
            
            db_name = score.source_table.split('.')[-1]
            sf_name = score.target_table.split('.')[-1]
            
            # Column matches
            col_html = ""
            if score.matching_columns:
                cols = [f"{s}→{t}" for s, t, _ in score.matching_columns[:3]]
                col_html = f'<div style="font-size: 11px; color: #a0a0b0; margin-top: 8px;">Columns: {", ".join(cols)}</div>'
            
            output += f"""
        <div style="background: {conf_bg}; border-radius: 10px; padding: 14px; display: grid; grid-template-columns: auto 1fr auto; gap: 16px; align-items: center;">
            <div style="font-size: 20px; font-weight: 800; color: {conf_color};">{score.total_score:.0%}</div>
            <div>
                <div style="display: flex; align-items: center; gap: 8px; font-size: 13px;">
                    <span style="color: #fdba74;">🧱 {db_name}</span>
                    <span style="color: #404050;">↔</span>
                    <span style="color: #93c5fd;">❄️ {sf_name}</span>
                </div>
                {col_html}
            </div>
            <div style="font-size: 11px; color: #606070; text-align: right;">
                <div>Sem: {score.column_semantic_score:.0%}</div>
                <div>Rel: {score.relationship_score:.0%}</div>
            </div>
        </div>
"""
        
        if len(results) > 10:
            output += f'<div style="text-align: center; padding: 12px; color: #606070; font-size: 12px;">... and {len(results) - 10} more matches</div>'
        
        output += """
    </div>
    
    <div style="margin-top: 16px; padding: 12px; background: rgba(99, 102, 241, 0.1); border-radius: 8px; display: flex; justify-content: space-between; font-size: 11px; color: #a0a0b0;">
        <span>40% Semantic</span>
        <span>25% Schema</span>
        <span>20% Statistical</span>
        <span>15% Relationship</span>
    </div>
</div>
"""
        return output, len(results)
        
    except Exception as e:
        return create_error_card(f"Detection error: {str(e)}"), 0


def save_cross_source_edges(threshold: float = 0.30):
    """Save cross-source matches to Neo4j"""
    if not has_cross_source:
        return create_error_card("Cross-Source Detector unavailable")
    
    try:
        results = cross_source_detector.detect_cross_source_duplicates(min_threshold=threshold)
        edges = cross_source_detector.create_similarity_edges(results, min_threshold=threshold)
        
        return f"""
<div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.08)); border-radius: 12px; padding: 20px; border: 1px solid rgba(16, 185, 129, 0.3); display: flex; align-items: center; gap: 16px;">
    <div style="font-size: 36px;">✅</div>
    <div>
        <div style="font-size: 16px; font-weight: 700; color: #6ee7b7;">Saved to Neo4j!</div>
        <div style="font-size: 13px; color: #a0a0b0;">{edges} SIMILAR_TO edges created at {threshold:.0%} threshold</div>
    </div>
</div>
"""
    except Exception as e:
        return create_error_card(f"Save error: {str(e)}")

def show_performance():
    if not has_eval_results:
        return create_error_card("Run: python scripts/run_comparative_evaluation.py")
    
    metrics = eval_results['metrics']
    sorted_systems = sorted(metrics.items(), key=lambda x: x[1]['success@1_rate'], reverse=True)
    colors = ['#10b981', '#3b82f6', '#8b5cf6', '#f59e0b', '#6b7280']
    
    output = """
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px;">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #3b82f6, #1d4ed8); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">📊</div>
        <div>
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5;">System Performance</div>
            <div style="font-size: 13px; color: #a0a0b0;">60-question benchmark evaluation</div>
        </div>
    </div>
    <div style="display: grid; gap: 12px;">
"""
    for idx, (name, m) in enumerate(sorted_systems[:5]):
        acc = m['success@1_rate'] * 100
        color = colors[idx]
        output += f"""
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 16px; border-left: 4px solid {color};">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <span style="font-size: 15px; font-weight: 600; color: #f0f0f5;">#{idx+1} {name}</span>
                <span style="font-size: 20px; font-weight: 700; color: {color};">{acc:.1f}%</span>
            </div>
            <div style="height: 8px; background: rgba(0,0,0,0.3); border-radius: 4px; overflow: hidden;">
                <div style="width: {acc}%; height: 100%; background: {color}; border-radius: 4px;"></div>
            </div>
        </div>
"""
    output += """
    </div>
    <div style="margin-top: 20px; padding-top: 16px; border-top: 1px solid rgba(255,255,255,0.06); display: flex; gap: 20px; font-size: 13px; color: #a0a0b0;">
        <span>📈 60 questions</span>
        <span>✅ p=0.027 (significant)</span>
    </div>
</div>
"""
    return output

def show_system_stats():
    with neo4j_driver.session() as session:
        # Existing Olist stats
        nodes = session.run("MATCH (n:OlistData) RETURN count(n) as c").single()['c']
        columns = session.run("MATCH (n:OlistColumn) RETURN count(n) as c").single()['c']
        dups = session.run("MATCH ()-[r:OLIST_DUPLICATE]->() RETURN count(r) as c").single()['c']
        lineage = session.run("MATCH ()-[r:DERIVES_FROM]->() RETURN count(r) as c").single()['c']
        customers = session.run("MATCH (c:Customer) RETURN count(c) as c").single()['c']
        orders = session.run("MATCH (o:Order) RETURN count(o) as c").single()['c']
        products = session.run("MATCH (p:Product) RETURN count(p) as c").single()['c']
        
        # NEW: Federated stats
        federated_tables = session.run("MATCH (t:FederatedTable) RETURN count(t) as c").single()['c']
        snowflake_tables = session.run("MATCH (t:FederatedTable {source: 'snowflake'}) RETURN count(t) as c").single()['c']
        databricks_tables = session.run("MATCH (t:FederatedTable {source: 'databricks'}) RETURN count(t) as c").single()['c']
        databricks_rows = session.run("MATCH (t:FederatedTable {source: 'databricks'}) RETURN sum(t.row_count) as c").single()['c'] or 0
        cross_source = session.run("MATCH ()-[r:SIMILAR_TO]-() RETURN count(r)/2 as c").single()['c']
        federated_columns = session.run("MATCH (c:FederatedColumn) RETURN count(c) as c").single()['c']
    
    return f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px;">
        
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 16px; padding: 24px; border: 1px solid rgba(59, 130, 246, 0.3);">
            <div style="font-size: 13px; font-weight: 600; color: #93c5fd; margin-bottom: 20px; display: flex; align-items: center; gap: 8px;">
                <span>❄️</span> SNOWFLAKE (OLIST)
            </div>
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{nodes}</div><div style="font-size: 12px; color: #606070;">Tables</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{columns}</div><div style="font-size: 12px; color: #606070;">Columns</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{dups}</div><div style="font-size: 12px; color: #606070;">Duplicates</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{lineage}</div><div style="font-size: 12px; color: #606070;">Lineage</div></div>
            </div>
        </div>
        
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 16px; padding: 24px; border: 1px solid rgba(249, 115, 22, 0.3);">
            <div style="font-size: 13px; font-weight: 600; color: #fdba74; margin-bottom: 20px; display: flex; align-items: center; gap: 8px;">
                <span>🧱</span> DATABRICKS
            </div>
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{databricks_tables}</div><div style="font-size: 12px; color: #606070;">Tables</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{federated_columns}</div><div style="font-size: 12px; color: #606070;">Columns</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{databricks_rows:,}</div><div style="font-size: 12px; color: #606070;">Rows</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{cross_source}</div><div style="font-size: 12px; color: #606070;">Cross-Links</div></div>
            </div>
        </div>
        
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 16px; padding: 24px; border: 1px solid rgba(255,255,255,0.06);">
            <div style="font-size: 13px; font-weight: 600; color: #a0a0b0; margin-bottom: 20px; display: flex; align-items: center; gap: 8px;">
                <span>💾</span> SAMPLE DATA (Neo4j)
            </div>
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;">
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{customers}</div><div style="font-size: 12px; color: #606070;">Customers</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{orders}</div><div style="font-size: 12px; color: #606070;">Orders</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">{products}</div><div style="font-size: 12px; color: #606070;">Products</div></div>
            </div>
        </div>
        
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 16px; padding: 24px; border: 1px solid rgba(16, 185, 129, 0.3);">
            <div style="font-size: 13px; font-weight: 600; color: #6ee7b7; margin-bottom: 20px; display: flex; align-items: center; gap: 8px;">
                <span>🎯</span> PERFORMANCE
            </div>
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px;">
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #10b981;">60%</div><div style="font-size: 12px; color: #606070;">Smart</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #f0f0f5;">53%</div><div style="font-size: 12px; color: #606070;">ML</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #10b981;">100%</div><div style="font-size: 12px; color: #606070;">Routing</div></div>
                <div style="text-align: center;"><div style="font-size: 32px; font-weight: 700; color: #10b981;">100%</div><div style="font-size: 12px; color: #606070;">Lineage F1</div></div>
            </div>
        </div>
        
        <div style="background: linear-gradient(135deg, rgba(6, 182, 212, 0.1), rgba(59, 130, 246, 0.05)); border-radius: 16px; padding: 24px; border: 1px solid rgba(6, 182, 212, 0.3); grid-column: span 2;">
            <div style="font-size: 13px; font-weight: 600; color: #67e8f9; margin-bottom: 20px; display: flex; align-items: center; gap: 8px;">
                <span>🌐</span> FEDERATION OVERVIEW
            </div>
            <div style="display: flex; justify-content: space-around; align-items: center;">
                <div style="text-align: center;">
                    <div style="font-size: 48px; font-weight: 800; color: #67e8f9;">{federated_tables}</div>
                    <div style="font-size: 14px; color: #a0a0b0;">Total Federated Tables</div>
                </div>
                <div style="font-size: 32px; color: #404050;">|</div>
                <div style="text-align: center;">
                    <div style="display: flex; gap: 24px;">
                        <div><span style="font-size: 28px; font-weight: 700; color: #93c5fd;">{snowflake_tables}</span><div style="font-size: 12px; color: #606070;">❄️ Snowflake</div></div>
                        <div><span style="font-size: 28px; font-weight: 700; color: #fdba74;">{databricks_tables}</span><div style="font-size: 12px; color: #606070;">🧱 Databricks</div></div>
                    </div>
                </div>
                <div style="font-size: 32px; color: #404050;">|</div>
                <div style="text-align: center;">
                    <div style="font-size: 28px; font-weight: 700; color: #6ee7b7;">{cross_source}</div>
                    <div style="font-size: 12px; color: #606070;">🔗 Cross-Source Links</div>
                </div>
            </div>
        </div>
        
    </div>
</div>
"""

# ========================================
# SHACL GOVERNANCE HANDLERS
# ========================================

def run_shacl_validation(scope: str = "all"):
    """Run SHACL validation with scope selection"""
    if not has_shacl:
        return create_error_card("SHACL Validator unavailable")
    try:
        # Run validation based on scope
        if scope == "snowflake":
            report = shacl_validator.validate_snowflake()
        elif scope == "databricks":
            report = shacl_validator.validate_databricks()
        elif scope == "federated":
            report = shacl_validator.validate_federated()
        elif scope == "cross-source":
            report = shacl_validator.validate_cross_source()
        else:
            report = shacl_validator.validate_all()
        
        return shacl_validator.generate_report_html(report)
    except Exception as e:
        return create_error_card(f"Validation error: {str(e)}")


def show_governance_shapes(scope_filter: str = "all"):
    """Show governance shapes with optional scope filtering"""
    if not has_shacl:
        return create_error_card("SHACL Validator unavailable")
    
    # Get shapes - filter by scope if specified
    if scope_filter == "all":
        shapes = shacl_validator.get_shape_info()
    else:
        shapes = shacl_validator.get_shape_info(scope=scope_filter)
    
    # Count by scope
    snowflake_count = len([s for s in shacl_validator.get_shape_info() if s.get('scope') == 'snowflake'])
    databricks_count = len([s for s in shacl_validator.get_shape_info() if s.get('scope') == 'databricks'])
    federated_count = len([s for s in shacl_validator.get_shape_info() if s.get('scope') == 'federated'])
    cross_source_count = len([s for s in shacl_validator.get_shape_info() if s.get('scope') == 'cross-source'])
    
    # Scope colors
    scope_colors = {
        'snowflake': ('#93c5fd', 'rgba(59, 130, 246, 0.2)', '❄️'),
        'databricks': ('#fdba74', 'rgba(249, 115, 22, 0.2)', '🧱'),
        'federated': ('#6ee7b7', 'rgba(16, 185, 129, 0.2)', '🌐'),
        'cross-source': ('#f9a8d4', 'rgba(236, 72, 153, 0.2)', '🔀'),
    }
    
    output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #8b5cf6, #6366f1); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">📋</div>
        <div style="flex: 1;">
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5;">Governance Shapes</div>
            <div style="font-size: 13px; color: #a0a0b0;">{len(shapes)} validation constraints {f'({scope_filter})' if scope_filter != 'all' else 'across all scopes'}</div>
        </div>
    </div>
    
    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px;">
        <div style="background: rgba(59, 130, 246, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(59, 130, 246, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #93c5fd;">{snowflake_count}</div>
            <div style="font-size: 11px; color: #a0a0b0;">❄️ Snowflake</div>
        </div>
        <div style="background: rgba(249, 115, 22, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(249, 115, 22, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #fdba74;">{databricks_count}</div>
            <div style="font-size: 11px; color: #a0a0b0;">🧱 Databricks</div>
        </div>
        <div style="background: rgba(16, 185, 129, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(16, 185, 129, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #6ee7b7;">{federated_count}</div>
            <div style="font-size: 11px; color: #a0a0b0;">🌐 Federated</div>
        </div>
        <div style="background: rgba(236, 72, 153, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(236, 72, 153, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #f9a8d4;">{cross_source_count}</div>
            <div style="font-size: 11px; color: #a0a0b0;">🔀 Cross-Source</div>
        </div>
    </div>
    
    <div style="display: grid; gap: 12px; max-height: 500px; overflow-y: auto;">
"""
    
    for shape in shapes:
        # Severity styling
        if shape['severity'] == 'critical':
            icon, border = "🔴", "#ef4444"
        elif shape['severity'] == 'warning':
            icon, border = "🟡", "#f59e0b"
        else:
            icon, border = "🔵", "#3b82f6"
        
        # Scope styling
        scope = shape.get('scope', 'unknown')
        scope_color, scope_bg, scope_icon = scope_colors.get(scope, ('#a0a0b0', 'rgba(160, 160, 176, 0.2)', '📦'))
        
        output += f"""
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 16px; border-left: 4px solid {border};">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span>{icon}</span>
                    <span style="font-weight: 600; color: #f0f0f5; font-size: 14px;">{shape['name']}</span>
                </div>
                <div style="display: flex; gap: 8px;">
                    <span style="background: {scope_bg}; padding: 4px 10px; border-radius: 8px; font-size: 10px; color: {scope_color};">{scope_icon} {scope.upper()}</span>
                    <span style="background: rgba(99, 102, 241, 0.2); padding: 4px 10px; border-radius: 8px; font-size: 10px; color: #a5b4fc; text-transform: uppercase;">{shape['severity']}</span>
                </div>
            </div>
            <div style="font-size: 13px; color: #a0a0b0;">{shape['description']}</div>
        </div>
"""
    output += "</div></div>"
    return output


def show_governance_stats():
    """Show governance validation statistics"""
    if not has_shacl:
        return create_error_card("SHACL Validator unavailable")
    
    try:
        stats = shacl_validator.get_stats()
        
        return f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 16px; padding: 24px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 20px;">
        <span style="font-size: 24px;">📊</span>
        <span style="font-size: 16px; font-weight: 600; color: #f0f0f5;">Governance Statistics</span>
    </div>
    
    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;">
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 20px; text-align: center;">
            <div style="font-size: 32px; font-weight: 700; color: #a5b4fc;">{stats['total_shapes']}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Total Shapes</div>
        </div>
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 20px; text-align: center;">
            <div style="font-size: 32px; font-weight: 700; color: #93c5fd;">{stats['snowflake_shapes']}</div>
            <div style="font-size: 12px; color: #a0a0b0;">❄️ Snowflake Shapes</div>
        </div>
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 20px; text-align: center;">
            <div style="font-size: 32px; font-weight: 700; color: #fdba74;">{stats['federated_shapes']}</div>
            <div style="font-size: 12px; color: #a0a0b0;">🌐 Federated Shapes</div>
        </div>
    </div>
    
    <div style="margin-top: 16px; display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px;">
        <div style="background: rgba(59, 130, 246, 0.1); border-radius: 12px; padding: 16px; border: 1px solid rgba(59, 130, 246, 0.2);">
            <div style="font-size: 12px; color: #93c5fd; margin-bottom: 8px;">❄️ Snowflake Coverage</div>
            <div style="display: flex; justify-content: space-between; font-size: 13px; color: #a0a0b0;">
                <span>OlistData Tables: {stats.get('olist_tables', 0)}</span>
                <span>OlistColumns: {stats.get('olist_columns', 0)}</span>
            </div>
        </div>
        <div style="background: rgba(249, 115, 22, 0.1); border-radius: 12px; padding: 16px; border: 1px solid rgba(249, 115, 22, 0.2);">
            <div style="font-size: 12px; color: #fdba74; margin-bottom: 8px;">🧱 Databricks Coverage</div>
            <div style="display: flex; justify-content: space-between; font-size: 13px; color: #a0a0b0;">
                <span>Federated Tables: {stats.get('federated_databricks', 0)}</span>
                <span>Federated Columns: {stats.get('federated_columns', 0)}</span>
            </div>
        </div>
    </div>
    
    <div style="margin-top: 16px; background: rgba(16, 185, 129, 0.1); border-radius: 12px; padding: 16px; border: 1px solid rgba(16, 185, 129, 0.2);">
        <div style="font-size: 12px; color: #6ee7b7; margin-bottom: 8px;">🔀 Cross-Source</div>
        <div style="font-size: 13px; color: #a0a0b0;">
            SIMILAR_TO Relationships: {stats.get('cross_source_matches', 0)}
        </div>
    </div>
</div>
"""
    except Exception as e:
        return create_error_card(f"Error: {str(e)}")

# ========================================
# FEDERATION HANDLERS
# ========================================

def show_federated_overview():
    """Show overview of federated knowledge graph"""
    if not has_federation:
        return create_error_card("Federation module unavailable")
    
    try:
        stats = federated_builder.get_federated_statistics()
        tables = federated_builder.get_all_federated_tables()
        
        # Count by source
        snowflake_count = stats.get('by_source', {}).get('snowflake', 0)
        databricks_count = stats.get('by_source', {}).get('databricks', 0)
        
        output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #06b6d4, #3b82f6); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">🌐</div>
        <div>
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5;">Federated Knowledge Graph</div>
            <div style="font-size: 13px; color: #a0a0b0;">Multi-source data catalog with privacy-preserving fingerprints</div>
        </div>
    </div>
    
    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px;">
        <div style="background: linear-gradient(135deg, rgba(6, 182, 212, 0.2), rgba(59, 130, 246, 0.1)); border-radius: 16px; padding: 20px; text-align: center; border: 1px solid rgba(6, 182, 212, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #67e8f9;">{stats['total_federated_tables']}</div>
            <div style="font-size: 13px; color: #a0a0b0;">Total Tables</div>
        </div>
        <div style="background: linear-gradient(135deg, rgba(59, 130, 246, 0.2), rgba(99, 102, 241, 0.1)); border-radius: 16px; padding: 20px; text-align: center; border: 1px solid rgba(59, 130, 246, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #93c5fd;">{snowflake_count}</div>
            <div style="font-size: 13px; color: #a0a0b0;">❄️ Snowflake</div>
        </div>
        <div style="background: linear-gradient(135deg, rgba(249, 115, 22, 0.2), rgba(234, 88, 12, 0.1)); border-radius: 16px; padding: 20px; text-align: center; border: 1px solid rgba(249, 115, 22, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #fdba74;">{databricks_count}</div>
            <div style="font-size: 13px; color: #a0a0b0;">🧱 Databricks</div>
        </div>
        <div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(5, 150, 105, 0.1)); border-radius: 16px; padding: 20px; text-align: center; border: 1px solid rgba(16, 185, 129, 0.3);">
            <div style="font-size: 36px; font-weight: 800; color: #6ee7b7;">{stats['cross_source_similarities']}</div>
            <div style="font-size: 13px; color: #a0a0b0;">🔗 Cross-Source Links</div>
        </div>
    </div>
    
    <div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.08)); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 12px; padding: 16px; margin-bottom: 24px; display: flex; align-items: center; gap: 12px;">
        <span style="font-size: 24px;">🔐</span>
        <div>
            <div style="font-size: 14px; font-weight: 600; color: #6ee7b7;">Privacy-Preserving Federation</div>
            <div style="font-size: 12px; color: #a0a0b0;">Only metadata fingerprints shared • No raw data transferred • Hash-based similarity matching</div>
        </div>
    </div>
    
    <div style="font-size: 14px; font-weight: 600; color: #a0a0b0; margin-bottom: 16px;">📋 FEDERATED TABLES</div>
    <div style="display: grid; gap: 12px; max-height: 400px; overflow-y: auto;">
"""
        
        for table in tables:
            # Source styling
            if table['source'] == 'snowflake':
                source_bg = "rgba(59, 130, 246, 0.2)"
                source_border = "rgba(59, 130, 246, 0.4)"
                source_icon = "❄️"
                source_color = "#93c5fd"
            else:
                source_bg = "rgba(249, 115, 22, 0.2)"
                source_border = "rgba(249, 115, 22, 0.4)"
                source_icon = "🧱"
                source_color = "#fdba74"
            
            # Similarity badge
            sim_badge = ""
            if table.get('similarity_count', 0) > 0:
                sim_badge = f'<span style="background: rgba(16, 185, 129, 0.3); padding: 2px 8px; border-radius: 8px; font-size: 11px; color: #6ee7b7;">🔗 {table["similarity_count"]} similar</span>'
            
            output += f"""
        <div style="background: {source_bg}; border: 1px solid {source_border}; border-radius: 12px; padding: 16px;">
            <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 8px;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span style="font-size: 20px;">{source_icon}</span>
                    <span style="font-size: 14px; font-weight: 600; color: #f0f0f5;">{table['table_name']}</span>
                </div>
                <div style="display: flex; gap: 8px;">
                    {sim_badge}
                    <span style="background: rgba(99, 102, 241, 0.2); padding: 2px 10px; border-radius: 8px; font-size: 11px; color: {source_color};">{table['source'].upper()}</span>
                </div>
            </div>
            <div style="font-size: 12px; color: #a0a0b0; margin-bottom: 8px;">{table['full_name']}</div>
            <div style="display: flex; gap: 16px; font-size: 12px; color: #606070;">
                <span>📊 {table['row_count']:,} rows</span>
                <span>📋 {table['column_count']} columns</span>
                {f"<span>👤 {table['owner']}</span>" if table.get('owner') else ""}
            </div>
        </div>
"""
        
        output += """
    </div>
</div>
"""
        return output
        
    except Exception as e:
        return create_error_card(f"Error loading federated data: {str(e)}")


def build_federation():
    """Build or rebuild the federated knowledge graph"""
    if not has_federation:
        return create_error_card("Federation module unavailable")
    
    try:
        # Run the federation builder
        stats = build_federated_graph()
        
        return f"""
<div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.08)); border-radius: 16px; padding: 24px; border: 1px solid rgba(16, 185, 129, 0.3);">
    <div style="text-align: center;">
        <div style="font-size: 48px; margin-bottom: 16px;">✅</div>
        <div style="font-size: 20px; font-weight: 700; color: #6ee7b7; margin-bottom: 8px;">Federated Graph Built Successfully!</div>
        <div style="font-size: 14px; color: #a0a0b0; margin-bottom: 16px;">
            {stats['total_federated_tables']} tables from {len(stats.get('by_source', {}))} sources
        </div>
        <div style="display: flex; justify-content: center; gap: 24px; font-size: 13px;">
            <span style="color: #93c5fd;">❄️ Snowflake: {stats.get('by_source', {}).get('snowflake', 0)}</span>
            <span style="color: #fdba74;">🧱 Databricks: {stats.get('by_source', {}).get('databricks', 0)}</span>
            <span style="color: #6ee7b7;">🔗 Similarities: {stats['cross_source_similarities']}</span>
        </div>
    </div>
</div>
"""
    except Exception as e:
        return create_error_card(f"Build failed: {str(e)}")


def show_cross_source_similarities():
    """Show cross-source table similarities"""
    if not has_federation:
        return create_error_card("Federation module unavailable")
    
    try:
        with neo4j_driver.session() as session:
            result = session.run("""
                MATCH (t1:FederatedTable)-[r:SIMILAR_TO]-(t2:FederatedTable)
                WHERE t1.source <> t2.source
                RETURN t1.full_name as table1, t1.source as source1,
                       t2.full_name as table2, t2.source as source2,
                       r.similarity as similarity, r.match_type as match_type
                ORDER BY r.similarity DESC
            """)
            similarities = list(result)
        
        if not similarities:
            return """
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 16px; padding: 48px; text-align: center;">
    <div style="font-size: 48px; margin-bottom: 16px;">🔍</div>
    <div style="font-size: 16px; color: #a0a0b0;">No cross-source similarities found yet</div>
    <div style="font-size: 13px; color: #606070; margin-top: 8px;">Build the federation first or lower similarity threshold</div>
</div>
"""
        
        output = f"""
<div style="background: rgba(20, 20, 30, 0.9); border-radius: 20px; padding: 28px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 24px;">
        <div style="width: 48px; height: 48px; background: linear-gradient(135deg, #10b981, #059669); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 24px;">🔗</div>
        <div>
            <div style="font-size: 18px; font-weight: 600; color: #f0f0f5;">Cross-Source Similarities</div>
            <div style="font-size: 13px; color: #a0a0b0;">{len(similarities)} potential matches found</div>
        </div>
    </div>
    <div style="display: grid; gap: 12px;">
"""
        
        for sim in similarities:
            # Source icons
            icon1 = "❄️" if sim['source1'] == 'snowflake' else "🧱"
            icon2 = "❄️" if sim['source2'] == 'snowflake' else "🧱"
            
            # Similarity color
            sim_pct = sim['similarity'] * 100
            if sim_pct >= 70:
                sim_color = "#6ee7b7"
            elif sim_pct >= 50:
                sim_color = "#fcd34d"
            else:
                sim_color = "#a0a0b0"
            
            output += f"""
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 16px; border-left: 4px solid {sim_color};">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                <span style="font-size: 20px; font-weight: 700; color: {sim_color};">{sim_pct:.0f}%</span>
                <span style="background: rgba(99, 102, 241, 0.2); padding: 4px 10px; border-radius: 8px; font-size: 11px; color: #a5b4fc;">{sim['match_type']}</span>
            </div>
            <div style="display: grid; grid-template-columns: 1fr auto 1fr; gap: 12px; align-items: center;">
                <div style="background: rgba(59, 130, 246, 0.1); padding: 12px; border-radius: 8px;">
                    <div style="font-size: 12px; color: #606070; margin-bottom: 4px;">{icon1} {sim['source1'].upper()}</div>
                    <div style="font-size: 13px; color: #f0f0f5; word-break: break-all;">{sim['table1'].split('.')[-1]}</div>
                </div>
                <div style="font-size: 20px; color: #606070;">↔️</div>
                <div style="background: rgba(249, 115, 22, 0.1); padding: 12px; border-radius: 8px;">
                    <div style="font-size: 12px; color: #606070; margin-bottom: 4px;">{icon2} {sim['source2'].upper()}</div>
                    <div style="font-size: 13px; color: #f0f0f5; word-break: break-all;">{sim['table2'].split('.')[-1]}</div>
                </div>
            </div>
        </div>
"""
        
        output += """
    </div>
</div>
"""
        return output
        
    except Exception as e:
        return create_error_card(f"Error: {str(e)}")

# ========================================
# GRADIO INTERFACE
# ========================================

theme = gr.themes.Base(
    primary_hue="indigo",
    secondary_hue="purple",
    neutral_hue="slate",
).set(
    body_background_fill="#0a0a0f",
    block_background_fill="#12121a",
    block_border_color="rgba(255,255,255,0.08)",
)

custom_css = """
.gradio-container { max-width: 1400px !important; margin: 0 auto !important; }
.tab-nav button.selected { background: linear-gradient(135deg, #6366f1, #8b5cf6) !important; color: white !important; }
button.primary { background: linear-gradient(135deg, #6366f1, #8b5cf6) !important; border: none !important; }
"""

with gr.Blocks(css=custom_css, title="NEXUS GraphRAG", theme=theme) as demo:
    
    gr.HTML("""
        <div style="padding: 48px; background: linear-gradient(135deg, rgba(99,102,241,0.15), rgba(139,92,246,0.1)); border-radius: 24px; margin-bottom: 24px; border: 1px solid rgba(255,255,255,0.08);">
            <div style="text-align: center;">
                <h1 style="margin: 0; font-size: 3rem; font-weight: 800; background: linear-gradient(135deg, #6366f1, #a855f7); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">NEXUS GraphRAG</h1>
                <p style="margin: 12px 0 0; color: #a0a0b0; font-size: 18px;">Knowledge Graph Data Catalog with Unified LLM Integration</p>
                <div style="margin-top: 20px; display: flex; justify-content: center; gap: 12px; flex-wrap: wrap;">
                    <span style="background: rgba(16,185,129,0.2); color: #6ee7b7; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 500;">✓ 60-70% Accuracy</span>
                    <span style="background: rgba(99,102,241,0.2); color: #a5b4fc; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 500;">✓ Unified System</span>
                    <span style="background: rgba(139,92,246,0.2); color: #c4b5fd; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 500;">✓ 100% Lineage F1</span>
                    <span style="background: rgba(139,92,246,0.2); color: #c4b5fd; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 500;">✓ SHACL-Inspired Governance </span>

                </div>
                <p style="margin: 20px 0 0; color: #606070; font-size: 14px;">🎓 Pranav Kharat • Northeastern University • MS Project 2025</p>
            </div>
        </div>
    """)
    
    with gr.Tabs():
        
        with gr.Tab("🌟 Unified Search"):
            gr.Markdown("### Ask Anything — Metadata or Sample Data")
            with gr.Row():
                with gr.Column(scale=4):
                    unified_input = gr.Textbox(label="Your Question", placeholder="Try: 'Which tables contain customer data?' or 'How many customers from São Paulo?'", lines=2)
                with gr.Column(scale=1):
                    unified_btn = gr.Button("🔍 Search", variant="primary", size="lg")
            gr.Examples(examples=[["Which tables contain customer data?"], ["What columns are in sales_transactions?"], ["How many customers from São Paulo?"], ["Tables with >100k rows?"], ["What does CLIENT_DATA derive from?"]], inputs=unified_input)
            unified_output = gr.HTML()
            unified_btn.click(query_unified, inputs=unified_input, outputs=unified_output)
            unified_input.submit(query_unified, inputs=unified_input, outputs=unified_output)
        
        with gr.Tab("🔗 Lineage Explorer"):
            gr.Markdown("### Data Lineage — Track Data Flow")
            with gr.Row():
                with gr.Column(scale=2):
                    lineage_table = gr.Dropdown(label="Select Table", choices=TABLE_CHOICES, value="CLIENT_DATA (Marketing)", interactive=True)
                with gr.Column(scale=2):
                    lineage_direction = gr.Radio(label="Direction", choices=["Upstream (Sources)", "Downstream (Targets)"], value="Upstream (Sources)")
                with gr.Column(scale=1):
                    lineage_btn = gr.Button("🔍 Explore", variant="primary")
            lineage_output = gr.HTML()
            lineage_btn.click(explore_lineage, inputs=[lineage_table, lineage_direction], outputs=lineage_output)
            lineage_table.change(explore_lineage, inputs=[lineage_table, lineage_direction], outputs=lineage_output)
            lineage_direction.change(explore_lineage, inputs=[lineage_table, lineage_direction], outputs=lineage_output)
            gr.Markdown("---")
            full_lineage_btn = gr.Button("🗺️ Show Complete Lineage Map", variant="secondary")
            full_lineage_output = gr.HTML()
            full_lineage_btn.click(show_full_lineage, outputs=full_lineage_output)
        
        with gr.Tab("⚔️ Compare Engines"):
            gr.Markdown("### Smart vs Learned GraphRAG")
            compare_input = gr.Textbox(label="Test Query", lines=2)
            compare_btn = gr.Button("⚔️ Compare", variant="primary")
            gr.Examples(examples=[["Which tables contain customer data?"], ["Find duplicate tables"]], inputs=compare_input)
            compare_output = gr.HTML()
            compare_btn.click(compare_engines, inputs=compare_input, outputs=compare_output)
        
        # UPDATED DUPLICATES TAB WITH EXPLAIN MATCH FEATURE
        with gr.Tab("🔄 Duplicates"):
            gr.Markdown("""
            ### Duplicate & Cross-Source Detection
            SANTOS algorithm finds similar tables within Snowflake and across platforms.
            """)
            
            with gr.Row():
                with gr.Column(scale=1):
                    dup_type = gr.Radio(
                        ["Within Snowflake", "Cross-Source (Databricks↔Snowflake)", "All"],
                        label="Detection Scope",
                        value="All"
                    )
                    dup_btn = gr.Button("🔍 Find Duplicates", variant="primary")
                
                with gr.Column(scale=2):
                    dup_output = gr.HTML(label="Results")
            
            dup_btn.click(show_duplicates, inputs=[dup_type], outputs=[dup_output])
            
            # NEW: Explain Match Section
            gr.Markdown("---")
            gr.Markdown("""
            ### 💡 Explain Why Tables Match
            Select a cross-source match to understand WHY the tables are similar.
            """)
            
            with gr.Row():
                with gr.Column(scale=1):
                    db_table_input = gr.Textbox(
                        label="Databricks Table",
                        placeholder="e.g., sales_transactions",
                        value="sales_transactions"
                    )
                    sf_table_input = gr.Textbox(
                        label="Snowflake Table", 
                        placeholder="e.g., ORDERS or OLIST_SALES.ORDERS",
                        value="ORDERS"
                    )
                    explain_btn = gr.Button("🔍 Explain Match", variant="secondary")
                
                with gr.Column(scale=2):
                    explain_output = gr.HTML(label="Explanation")
            
            explain_btn.click(
                explain_cross_source_match,
                inputs=[db_table_input, sf_table_input],
                outputs=[explain_output]
            )
            
            # Quick explain buttons for known matches
            gr.Markdown("**Quick Explain:**")
            with gr.Row():
                quick_btn1 = gr.Button("sales_transactions ↔ ORDERS", size="sm")
                quick_btn2 = gr.Button("customer_feedback ↔ ORDER_REVIEWS", size="sm")
                quick_btn3 = gr.Button("sales_transactions ↔ ORDER_ITEMS", size="sm")
            
            quick_btn1.click(
                lambda: explain_cross_source_match("sales_transactions", "ORDERS"),
                outputs=[explain_output]
            )
            quick_btn2.click(
                lambda: explain_cross_source_match("customer_feedback", "ORDER_REVIEWS"),
                outputs=[explain_output]
            )
            quick_btn3.click(
                lambda: explain_cross_source_match("sales_transactions", "ORDER_ITEMS"),
                outputs=[explain_output]
            )
        
        with gr.Tab("📊 Performance"):
            gr.Markdown("### System Performance Benchmarks")
            perf_btn = gr.Button("📈 Load Results", variant="primary")
            perf_output = gr.HTML()
            perf_btn.click(show_performance, outputs=perf_output)
        
        with gr.Tab("⚙️ System"):
            gr.Markdown("### System Overview")
            sys_output = gr.HTML()
            demo.load(show_system_stats, outputs=sys_output)
            refresh_btn = gr.Button("🔄 Refresh", variant="secondary")
            refresh_btn.click(show_system_stats, outputs=sys_output)
        
        with gr.Tab("🛡️ Governance"):
            gr.Markdown("### SHACL-Inspired Data Governance")
            gr.Markdown("Validate your knowledge graph against governance constraints.")
            with gr.Row():
                with gr.Column(scale=3):
                    validation_scope = gr.Radio(
                        label="Validation Scope",
                        choices=["all", "snowflake", "databricks", "federated", "cross-source"],
                        value="all",
                        info="Select which constraints to run"
                    )
                with gr.Column(scale=1):
                    validate_btn = gr.Button("🔍 Run Validation", variant="primary", size="lg")
            
            # Scope descriptions
            gr.HTML("""
                <div style="display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0;">
                    <span style="background: rgba(139, 92, 246, 0.2); padding: 6px 12px; border-radius: 8px; font-size: 12px; color: #c4b5fd;">🟣 ALL: 20 shapes</span>
                    <span style="background: rgba(59, 130, 246, 0.2); padding: 6px 12px; border-radius: 8px; font-size: 12px; color: #93c5fd;">❄️ Snowflake: 10 shapes</span>
                    <span style="background: rgba(249, 115, 22, 0.2); padding: 6px 12px; border-radius: 8px; font-size: 12px; color: #fdba74;">🧱 Databricks: 6 shapes</span>
                    <span style="background: rgba(16, 185, 129, 0.2); padding: 6px 12px; border-radius: 8px; font-size: 12px; color: #6ee7b7;">🌐 Federated: 1 shape</span>
                    <span style="background: rgba(236, 72, 153, 0.2); padding: 6px 12px; border-radius: 8px; font-size: 12px; color: #f9a8d4;">🔀 Cross-Source: 3 shapes</span>
                </div>
            """)
            
            governance_output = gr.HTML()
            validate_btn.click(run_shacl_validation, inputs=[validation_scope], outputs=governance_output)
            
            gr.Markdown("---")
            
            # Shapes viewer with filtering
            gr.Markdown("### 📋 Governance Shape Definitions")
            with gr.Row():
                with gr.Column(scale=3):
                    shape_filter = gr.Dropdown(
                        label="Filter by Scope",
                        choices=["all", "snowflake", "databricks", "federated", "cross-source"],
                        value="all",
                        interactive=True
                    )
                with gr.Column(scale=1):
                    shapes_btn = gr.Button("📋 View Shapes", variant="secondary")
                with gr.Column(scale=1):
                    stats_btn = gr.Button("📊 View Stats", variant="secondary")
            
            shapes_output = gr.HTML()
            
            shapes_btn.click(show_governance_shapes, inputs=[shape_filter], outputs=shapes_output)
            shape_filter.change(show_governance_shapes, inputs=[shape_filter], outputs=shapes_output)
            stats_btn.click(show_governance_stats, outputs=shapes_output)
            
            # Load shapes on tab open
            demo.load(lambda: show_governance_shapes("all"), outputs=shapes_output)

        with gr.Tab("🌐 Federation"):
            gr.Markdown("### Multi-Source Federated Knowledge Graph")
            gr.Markdown("Privacy-preserving federation of Snowflake + Databricks metadata")
            
            with gr.Row():
                build_fed_btn = gr.Button("🔨 Build Federation", variant="primary")
                refresh_fed_btn = gr.Button("🔄 Refresh View", variant="secondary")
            
            federation_output = gr.HTML()
            
            build_fed_btn.click(build_federation, outputs=federation_output)
            refresh_fed_btn.click(show_federated_overview, outputs=federation_output)
            
            gr.Markdown("---")
            gr.Markdown("### 🔗 Cross-Source Discoveries")
            similarities_btn = gr.Button("Show Similar Tables Across Sources", variant="secondary")
            similarities_output = gr.HTML()
            similarities_btn.click(show_cross_source_similarities, outputs=similarities_output)
            
            # Load overview on tab open
            demo.load(show_federated_overview, outputs=federation_output)
        
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 NEXUS GraphRAG Demo v2.0")
    print("="*60)
    print("🌐 URL: http://localhost:7860")
    print("="*60 + "\n")
    demo.launch(server_port=7860, share=False, show_error=True)