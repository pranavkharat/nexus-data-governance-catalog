# scripts/extract_lineage.py

"""
Main script to extract and build data lineage.

Usage:
    python scripts/extract_lineage.py

This script:
1. Extracts lineage from Snowflake (query history, dependencies, schema analysis)
2. Builds DERIVES_FROM relationships in Neo4j
3. Evaluates extraction accuracy against ground truth
4. Prints comprehensive report
"""

import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.lineage.snowflake_lineage_extractor import SnowflakeLineageExtractor
from src.lineage.lineage_graph_builder import LineageGraphBuilder
from datetime import datetime
import json


def main():
    """Main extraction pipeline."""
    
    print("\n" + "🔗"*35)
    print("NEXUS DATA LINEAGE EXTRACTION PIPELINE")
    print("🔗"*35)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # =========================================
    # STEP 1: EXTRACT LINEAGE FROM SNOWFLAKE
    # =========================================
    
    print("\n" + "="*70)
    print("STEP 1: EXTRACTING LINEAGE FROM SNOWFLAKE")
    print("="*70)
    
    extractor = SnowflakeLineageExtractor()
    lineage_edges = extractor.extract_all_lineage()
    
    print(f"\n📊 Extracted {len(lineage_edges)} lineage edges")
    
    # =========================================
    # STEP 2: BUILD LINEAGE GRAPH IN NEO4J
    # =========================================
    
    print("\n" + "="*70)
    print("STEP 2: BUILDING LINEAGE GRAPH IN NEO4J")
    print("="*70)
    
    builder = LineageGraphBuilder()
    build_stats = builder.build_lineage_graph(lineage_edges)
    
    # =========================================
    # STEP 3: EVALUATE EXTRACTION ACCURACY
    # =========================================
    
    print("\n" + "="*70)
    print("STEP 3: EVALUATING EXTRACTION ACCURACY")
    print("="*70)
    
    metrics = extractor.evaluate_extraction(lineage_edges)
    
    print(f"\n📈 EVALUATION METRICS (RQ2):")
    print(f"   Precision: {metrics['precision']:.2%}")
    print(f"   Recall: {metrics['recall']:.2%}")
    print(f"   F1 Score: {metrics['f1']:.2%}")
    print(f"\n   Ground Truth Edges: {metrics['ground_truth_count']}")
    print(f"   True Positives: {metrics['true_positives']}")
    print(f"   False Positives: {metrics['false_positives']}")
    print(f"   False Negatives: {metrics['false_negatives']}")
    
    # =========================================
    # STEP 4: TEST LINEAGE QUERIES
    # =========================================
    
    print("\n" + "="*70)
    print("STEP 4: TESTING LINEAGE QUERIES")
    print("="*70)
    
    # Test upstream
    print("\n🔼 UPSTREAM: What does CLIENT_DATA derive from?")
    upstream = builder.get_upstream_lineage('OLIST_MARKETING', 'CLIENT_DATA')
    for table in upstream:
        print(f"   ← {table['schema']}.{table['table']} ({table['lineage_type']}, {table['confidence']:.0%})")
    
    print("\n🔼 UPSTREAM: What does CUSTOMER_MASTER derive from?")
    upstream = builder.get_upstream_lineage('OLIST_ANALYTICS', 'CUSTOMER_MASTER')
    for table in upstream:
        print(f"   ← {table['schema']}.{table['table']} ({table['lineage_type']}, {table['confidence']:.0%})")
    
    # Test downstream
    print("\n🔽 DOWNSTREAM: What tables derive from CUSTOMERS?")
    downstream = builder.get_downstream_lineage('OLIST_SALES', 'CUSTOMERS')
    for table in downstream:
        print(f"   → {table['schema']}.{table['table']} ({table['lineage_type']}, {table['confidence']:.0%})")
    
    print("\n🔽 DOWNSTREAM: What tables derive from ORDERS?")
    downstream = builder.get_downstream_lineage('OLIST_SALES', 'ORDERS')
    for table in downstream:
        print(f"   → {table['schema']}.{table['table']} ({table['lineage_type']}, {table['confidence']:.0%})")
    
    # =========================================
    # STEP 5: GENERATE REPORT
    # =========================================
    
    print("\n" + "="*70)
    print("STEP 5: LINEAGE REPORT")
    print("="*70)
    
    graph = builder.get_full_lineage_graph()
    stats = builder.get_lineage_statistics()
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'extraction': {
            'total_edges': len(lineage_edges),
            'by_type': {}
        },
        'graph': {
            'nodes': graph['node_count'],
            'edges': graph['edge_count']
        },
        'evaluation': metrics,
        'lineage_edges': [
            {
                'source': f"{e.source_schema}.{e.source_table}",
                'target': f"{e.target_schema}.{e.target_table}",
                'type': e.lineage_type,
                'confidence': e.confidence
            }
            for e in lineage_edges
        ]
    }
    
    # Count by type
    for edge in lineage_edges:
        t = edge.lineage_type
        report['extraction']['by_type'][t] = report['extraction']['by_type'].get(t, 0) + 1
    
    # Save report
    report_path = os.path.join(project_root, 'data', 'evaluation', 'lineage_report.json')
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n📄 Report saved to: {report_path}")
    
    # Print summary
    print("\n" + "="*70)
    print("📊 LINEAGE EXTRACTION SUMMARY")
    print("="*70)
    print(f"""
    EXTRACTION:
    • Total lineage edges: {len(lineage_edges)}
    • By type: {report['extraction']['by_type']}
    
    GRAPH:
    • Nodes involved: {graph['node_count']}
    • DERIVES_FROM edges: {graph['edge_count']}
    
    EVALUATION (RQ2 METRICS):
    • Precision: {metrics['precision']:.2%}
    • Recall: {metrics['recall']:.2%}  
    • F1 Score: {metrics['f1']:.2%}
    
    STATUS: {'✅ PASSED' if metrics['f1'] >= 0.85 else '⚠️ BELOW TARGET'} (Target F1 ≥ 0.85)
    """)
    
    # =========================================
    # CLEANUP
    # =========================================
    
    extractor.close()
    builder.close()
    
    print(f"\n✅ Lineage extraction complete!")
    print(f"   Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return report


if __name__ == "__main__":
    main()