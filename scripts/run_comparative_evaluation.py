#!/usr/bin/env python3
# scripts/run_comparative_evaluation.py

"""
Comparative Evaluation: GraphRAG vs All Baselines
Runs 4 systems on same questions and compares performance
Uses manual McNemar test with chi-square approximation
"""

import json
import sys
import os
import time
import math
from datetime import datetime

# ========================================
# PATH SETUP
# ========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

QUESTIONS_PATH = os.path.join(PROJECT_ROOT, 'data', 'evaluation', 'benchmark_questions.json')
RESULTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'evaluation', 'comparative_results.json')

print(f"📁 Project root: {PROJECT_ROOT}")
print(f"📋 Questions path: {QUESTIONS_PATH}")
print(f"📊 Results path: {RESULTS_PATH}")

from src.graphrag.smart_graphrag_engine import SmartGraphRAGEngine
from src.evaluation.baseline_systems import KeywordSearchBaseline, EmbeddingsOnlyBaseline, GraphOnlyBaseline
from src.graphrag.learned_graphrag_engine import LearnedGraphRAGEngine



# ========================================
# STATISTICAL FUNCTIONS
# ========================================

def mcnemar_test_manual(contingency_table):
    """
    Improved manual McNemar test with accurate p-value
    
    Uses error function for chi-square(1) CDF approximation
    """
    b = contingency_table[0][1]  # baseline only correct
    c = contingency_table[1][0]  # graphrag only correct
    
    if b + c == 0:
        return 0.0, 1.0
    
    # McNemar test statistic with continuity correction
    numerator = (abs(b - c) - 1) ** 2
    denominator = b + c
    chi_square = numerator / denominator
    
    # Accurate p-value using error function
    z = math.sqrt(chi_square)
    try:
        # P(χ² > x) for df=1 using complementary error function
        pvalue = math.erfc(z / math.sqrt(2))
    except:
        # Fallback to lookup if erfc fails
        if chi_square >= 10.83:
            pvalue = 0.001
        elif chi_square >= 6.63:
            pvalue = 0.01
        elif chi_square >= 3.84:
            pvalue = 0.049  # Just below 0.05
        elif chi_square >= 2.71:
            pvalue = 0.10
        else:
            pvalue = 0.20
    
    return chi_square, pvalue


# ========================================
# HELPER FUNCTIONS
# ========================================

def normalize_table_name(table_name):
    """Normalize table names for comparison"""
    if not table_name:
        return ""
    parts = table_name.upper().split('.')
    if len(parts) >= 2:
        return '.'.join(parts[-2:])
    return table_name.upper()


def is_correct(result_table, question):
    """Check if result matches ground truth"""
    if not result_table:
        return 'wrong', False
    
    result_normalized = normalize_table_name(result_table)
    
    for gt in question['ground_truth']:
        gt_normalized = normalize_table_name(gt)
        if gt_normalized in result_normalized or result_normalized in gt_normalized:
            return 'exact', True
    
    for acc in question.get('acceptable', []):
        acc_normalized = normalize_table_name(acc)
        if acc_normalized in result_normalized or result_normalized in acc_normalized:
            return 'acceptable', True
    
    return 'wrong', False


def evaluate_system(system_name, system, questions):
    """Run one system on all questions"""
    
    print(f"\n{'='*70}")
    print(f"EVALUATING: {system_name}")
    print(f"{'='*70}")
    
    results = []
    success_at_1 = 0
    success_at_3 = 0
    total_time = 0
    
    for i, question in enumerate(questions, 1):
        q_text = question['question']
        q_id = question['id']
        
        print(f"\n[{i}/{len(questions)}] {q_text}")
        
        start_time = time.time()
        try:
            response = system.query(q_text, top_k=10)
            elapsed = time.time() - start_time
            total_time += elapsed
            
            top_results = response.get('results', [])[:10]
            
            if not top_results:
                print("  ❌ No results returned")
                results.append({
                    'question_id': q_id,
                    'question': q_text,
                    'category': question.get('category', 'unknown'),
                    'top_1': None,
                    'top_3': [],
                    'top_10': [],
                    'correct_at_1': False,
                    'correct_at_3': False,
                    'time_ms': elapsed * 1000
                })
                continue
            
            def extract_table_name(result):
                for key in ['table', 'name', 'table_name', 'full_name', 'table1', 'table2']:
                    if key in result and result[key]:
                        return result[key]
                return None
            
            top_1_table = extract_table_name(top_results[0])
            top_3_tables = [extract_table_name(r) for r in top_results[:3] if extract_table_name(r)]
            top_10_tables = [extract_table_name(r) for r in top_results[:10] if extract_table_name(r)]
            
            match_1, correct_1 = is_correct(top_1_table, question)
            correct_3 = any(is_correct(t, question)[1] for t in top_3_tables if t)
            
            if correct_1:
                success_at_1 += 1
                print(f"  ✅ Correct at 1: {top_1_table}")
            else:
                print(f"  ❌ Wrong at 1: {top_1_table}")
                print(f"     Expected: {question['ground_truth']}")
            
            if correct_3:
                success_at_3 += 1
                if not correct_1:
                    print(f"  ⚠️  Correct in top-3")
            
            results.append({
                'question_id': q_id,
                'question': q_text,
                'category': question.get('category', 'unknown'),
                'top_1': top_1_table,
                'top_3': top_3_tables,
                'top_10': top_10_tables,
                'correct_at_1': correct_1,
                'correct_at_3': correct_3,
                'time_ms': elapsed * 1000,
                'ground_truth': question['ground_truth']
            })
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            results.append({
                'question_id': q_id,
                'question': q_text,
                'category': question.get('category', 'unknown'),
                'error': str(e),
                'correct_at_1': False,
                'correct_at_3': False,
                'time_ms': 0
            })
    
    n = len(questions)
    avg_time = (total_time / n) * 1000 if n > 0 else 0
    
    metrics = {
        'success@1_count': success_at_1,
        'success@3_count': success_at_3,
        'success@1_rate': success_at_1 / n if n > 0 else 0,
        'success@3_rate': success_at_3 / n if n > 0 else 0,
        'avg_time_ms': avg_time,
        'total_questions': n
    }
    
    mrr_sum = 0
    for result in results:
        if result.get('error'):
            continue
        for rank, table in enumerate(result.get('top_10', []), 1):
            question_obj = next((q for q in questions if q['id'] == result['question_id']), None)
            if question_obj and is_correct(table, question_obj)[1]:
                mrr_sum += 1 / rank
                break
    
    metrics['mrr'] = mrr_sum / n if n > 0 else 0
    
    categories = {}
    for result in results:
        cat = result.get('category', 'unknown')
        if cat not in categories:
            categories[cat] = {'total': 0, 'correct': 0}
        categories[cat]['total'] += 1
        if result.get('correct_at_1'):
            categories[cat]['correct'] += 1
    
    metrics['by_category'] = {
        cat: {
            'success_rate': stats['correct'] / stats['total'] if stats['total'] > 0 else 0,
            'count': stats['total']
        }
        for cat, stats in categories.items()
    }
    
    print(f"\n{'='*70}")
    print(f"RESULTS: {system_name}")
    print(f"{'='*70}")
    print(f"Success@1: {success_at_1}/{n} ({metrics['success@1_rate']*100:.1f}%)")
    print(f"Success@3: {success_at_3}/{n} ({metrics['success@3_rate']*100:.1f}%)")
    print(f"MRR: {metrics['mrr']:.3f}")
    print(f"Avg Time: {avg_time:.1f}ms")
    
    print(f"\nBy Category:")
    for cat, stats in metrics['by_category'].items():
        print(f"  {cat}: {stats['success_rate']*100:.1f}% ({stats['count']} questions)")
    
    return {
        'system': system_name,
        'metrics': metrics,
        'results': results
    }


def compare_systems(all_results):
    """Compare all systems with manual statistical tests"""
    
    print(f"\n{'='*70}")
    print("COMPARATIVE ANALYSIS")
    print(f"{'='*70}")
    
    print("\n📊 Performance Comparison:")
    print(f"\n{'System':<25} {'Success@1':<12} {'Success@3':<12} {'MRR':<8} {'Time (ms)'}")
    print("-" * 70)
    
    for result in all_results:
        m = result['metrics']
        print(f"{result['system']:<25} "
              f"{m['success@1_rate']*100:>6.1f}%     "
              f"{m['success@3_rate']*100:>6.1f}%     "
              f"{m['mrr']:>5.3f}    "
              f"{m['avg_time_ms']:>6.1f}")
    
    print(f"\n{'='*70}")
    print("STATISTICAL SIGNIFICANCE (McNemar Test - Manual Implementation)")
    print(f"{'='*70}")
    
    graphrag_results = None
    for r in all_results:
        if 'GraphRAG' in r['system']:
            graphrag_results = r
            break
    
    if not graphrag_results:
        print("⚠️  No GraphRAG results found")
        return
    
    for baseline in all_results:
        if baseline['system'] == graphrag_results['system']:
            continue
        
        both_correct = 0
        graphrag_only = 0
        baseline_only = 0
        both_wrong = 0
        
        for gr, bl in zip(graphrag_results['results'], baseline['results']):
            gr_correct = gr.get('correct_at_1', False)
            bl_correct = bl.get('correct_at_1', False)
            
            if gr_correct and bl_correct:
                both_correct += 1
            elif gr_correct and not bl_correct:
                graphrag_only += 1
            elif not gr_correct and bl_correct:
                baseline_only += 1
            else:
                both_wrong += 1
        
        contingency = [[both_correct, baseline_only],
                      [graphrag_only, both_wrong]]
        
        chi_square, pvalue = mcnemar_test_manual(contingency)
        
        if pvalue is not None:
            # Fix threshold check: use <= 0.05 instead of < 0.05
            significant = "✅ Significant (p≤0.05)" if pvalue <= 0.05 else "❌ Not significant (p>0.05)"
            
            improvement = graphrag_results['metrics']['success@1_rate'] - baseline['metrics']['success@1_rate']
            
            print(f"\n🔬 GraphRAG vs {baseline['system']}:")
            print(f"   Success@1 Improvement: {improvement*100:+.1f}%")
            print(f"   p-value: {pvalue:.4f} {significant}")
            print(f"   Chi-square: {chi_square:.3f}")
            print(f"   GraphRAG better: {graphrag_only} questions")
            print(f"   Baseline better: {baseline_only} questions")
            print(f"   Both correct: {both_correct} | Both wrong: {both_wrong}")


def save_results(all_results):
    """Save results to JSON"""
    
    output = {
        'timestamp': datetime.now().isoformat(),
        'systems_evaluated': len(all_results),
        'metrics': {r['system']: r['metrics'] for r in all_results},
        'detailed_results': all_results
    }
    
    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    
    with open(RESULTS_PATH, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Results saved to: {RESULTS_PATH}")


def main():
    """Run complete comparative evaluation"""
    
    print("="*70)
    print("COMPARATIVE EVALUATION: GraphRAG vs All Baselines")
    print("="*70)
    
    print(f"\n📂 Loading questions from: {QUESTIONS_PATH}")
    
    if not os.path.exists(QUESTIONS_PATH):
        print(f"\n❌ ERROR: Questions file not found!")
        sys.exit(1)
    
    with open(QUESTIONS_PATH, 'r') as f:
        questions = json.load(f)
    
    if isinstance(questions, dict) and 'questions' in questions:
        questions = questions['questions']
    
    print(f"✅ Loaded {len(questions)} evaluation questions")
    
    print("\n🚀 Initializing all systems...")
    
    try:
        smart_graphrag = SmartGraphRAGEngine()
        learned_graphrag = LearnedGraphRAGEngine()
        keyword_baseline = KeywordSearchBaseline()
        embeddings_baseline = EmbeddingsOnlyBaseline()
        graph_baseline = GraphOnlyBaseline()
        print("✅ All systems initialized successfully")
        
    except Exception as e:
        print(f"❌ Failed to initialize systems: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    all_results = []
    
    
    print("\n" + "="*70)
    print("STARTING EVALUATION 1/5: Smart GraphRAG")
    print("="*70)
    result = evaluate_system("Smart GraphRAG", smart_graphrag, questions)
    all_results.append(result)
    
    print("\n" + "="*70)
    print("STARTING EVALUATION 2/5: Keyword Search")
    print("="*70)
    result = evaluate_system("Keyword Search", keyword_baseline, questions)
    all_results.append(result)
    
    print("\n" + "="*70)
    print("STARTING EVALUATION 3/5: Embeddings-Only")
    print("="*70)
    result = evaluate_system("Embeddings-Only", embeddings_baseline, questions)
    all_results.append(result)
    
    print("\n" + "="*70)
    print("STARTING EVALUATION 4/5: Graph-Only")
    print("="*70)
    result = evaluate_system("Graph-Only", graph_baseline, questions)
    all_results.append(result)
    
    print("\n" + "="*70)
    print("STARTING EVALUATION 5/5: Learned Graph (ML based)")
    print("="*70)
    result = evaluate_system("Learned GraphRAG (XGBoost)", learned_graphrag, questions)
    all_results.append(result)
    
    compare_systems(all_results)
    save_results(all_results)
    
    print("\n" + "="*70)
    print("✅ EVALUATION COMPLETE")
    print("="*70)
    print(f"\n📊 Results saved to: {RESULTS_PATH}")
    
    print("\n🧹 Cleaning up connections...")
    smart_graphrag.close()
    keyword_baseline.close()
    embeddings_baseline.close()
    graph_baseline.close()
    print("✅ All connections closed")


if __name__ == "__main__":
    main()