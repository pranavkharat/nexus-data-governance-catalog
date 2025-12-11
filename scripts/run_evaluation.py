import json
import sys
import os
import time
from datetime import datetime

# Fix import path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.graphrag.graphrag_engine import GraphRAGEngine
from src.graphrag.smart_graphrag_engine import SmartGraphRAGEngine

# ============================================================================
# EVALUATION FUNCTIONS
# ============================================================================

def is_correct(result_table, question):
    """
    Check if result matches ground truth
    Returns: (match_type, is_correct)
    """
    # Check ground truth (primary answer)
    for gt in question['ground_truth']:
        if gt.upper() in result_table.upper():
            return 'exact', True
    
    # Check acceptable (secondary answers)
    for acc in question.get('acceptable', []):
        if acc.upper() in result_table.upper():
            return 'acceptable', True
    
    return 'wrong', False

def evaluate_graphrag(questions, engine):
    """Run GraphRAG on all questions and collect metrics"""
    
    results = []
    
    print("\n" + "="*70)
    print("RUNNING GRAPHRAG EVALUATION")
    print("="*70 + "\n")
    
    for q in questions:
        print(f"Question {q['id']}/{len(questions)}: {q['question']}")
        
        start_time = time.time()
        
        try:
            # Get GraphRAG answer
            answer = engine.query(q['question'], top_k=5)
            
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            
            # Extract top results
            top_1 = answer['results'][0]['table'] if answer['results'] else None
            top_3 = [r['table'] for r in answer['results'][:3]]
            
            # Check correctness
            match_type_1, success_1 = is_correct(top_1, q)
            
            # Success@3: Check if ANY of top 3 are correct
            success_3 = False
            for table in top_3:
                match_type, is_match = is_correct(table, q)
                if is_match:
                    success_3 = True
                    break
            
            # Store result
            result = {
                'question_id': q['id'],
                'question': q['question'],
                'category': q['category'],
                'difficulty': q['difficulty'],
                'top_1': top_1,
                'top_3': top_3,
                'ground_truth': q['ground_truth'],
                'match_type': match_type_1,
                'success@1': success_1,
                'success@3': success_3,
                'response_time_ms': round(response_time, 2)
            }
            
            results.append(result)
            
            # Print result
            status_1 = "✅" if success_1 else "❌"
            status_3 = "✅" if success_3 else "❌"
            print(f"  Top-1: {top_1}")
            print(f"  Success@1: {status_1} ({match_type_1})")
            print(f"  Success@3: {status_3}")
            print(f"  Response time: {response_time:.1f}ms")
            print()
            
        except Exception as e:
            print(f"  ❌ ERROR: {e}\n")
            results.append({
                'question_id': q['id'],
                'question': q['question'],
                'error': str(e),
                'success@1': False,
                'success@3': False
            })
    
    return results

def calculate_metrics(results):
    """Calculate aggregate metrics"""
    
    total = len(results)
    
    # Overall metrics
    exact_match_1 = sum(1 for r in results if r.get('match_type') == 'exact' and r.get('success@1'))
    acceptable_match_1 = sum(1 for r in results if r.get('match_type') == 'acceptable' and r.get('success@1'))
    success_1 = sum(1 for r in results if r.get('success@1', False))
    success_3 = sum(1 for r in results if r.get('success@3', False))
    
    # By category
    categories = {}
    for r in results:
        cat = r.get('category', 'unknown')
        if cat not in categories:
            categories[cat] = {'total': 0, 'success@1': 0, 'success@3': 0}
        categories[cat]['total'] += 1
        if r.get('success@1'):
            categories[cat]['success@1'] += 1
        if r.get('success@3'):
            categories[cat]['success@3'] += 1
    
    # By difficulty
    difficulties = {}
    for r in results:
        diff = r.get('difficulty', 'unknown')
        if diff not in difficulties:
            difficulties[diff] = {'total': 0, 'success@1': 0, 'success@3': 0}
        difficulties[diff]['total'] += 1
        if r.get('success@1'):
            difficulties[diff]['success@1'] += 1
        if r.get('success@3'):
            difficulties[diff]['success@3'] += 1
    
    # Average response time
    response_times = [r.get('response_time_ms', 0) for r in results if 'response_time_ms' in r]
    avg_response_time = sum(response_times) / len(response_times) if response_times else 0
    
    # Mean Reciprocal Rank (MRR)
    reciprocal_ranks = []
    for r in results:
        if r.get('success@1'):
            reciprocal_ranks.append(1.0)
        elif r.get('success@3'):
            # Assume it's in position 2 or 3 (conservative estimate: 1/2 = 0.5)
            reciprocal_ranks.append(0.5)
        else:
            reciprocal_ranks.append(0.0)
    
    mrr = sum(reciprocal_ranks) / len(reciprocal_ranks) if reciprocal_ranks else 0
    
    return {
        'total_questions': total,
        'exact_match@1': exact_match_1,
        'acceptable_match@1': acceptable_match_1,
        'success@1': success_1,
        'success@1_rate': success_1 / total if total > 0 else 0,
        'success@3': success_3,
        'success@3_rate': success_3 / total if total > 0 else 0,
        'mrr': mrr,
        'avg_response_time_ms': avg_response_time,
        'by_category': categories,
        'by_difficulty': difficulties
    }

def print_report(metrics):
    """Print formatted evaluation report"""
    
    print("\n" + "="*70)
    print("📊 GRAPHRAG EVALUATION REPORT")
    print("="*70)
    
    print(f"\n📈 Overall Metrics:")
    print(f"  Total Questions: {metrics['total_questions']}")
    print(f"  Exact Match @1: {metrics['exact_match@1']} ({metrics['exact_match@1']/metrics['total_questions']*100:.1f}%)")
    print(f"  Acceptable Match @1: {metrics['acceptable_match@1']} ({metrics['acceptable_match@1']/metrics['total_questions']*100:.1f}%)")
    print(f"  Success @1: {metrics['success@1']} ({metrics['success@1_rate']*100:.1f}%)")
    print(f"  Success @3: {metrics['success@3']} ({metrics['success@3_rate']*100:.1f}%)")
    print(f"  MRR (Mean Reciprocal Rank): {metrics['mrr']:.3f}")
    print(f"  Avg Response Time: {metrics['avg_response_time_ms']:.1f}ms")
    
    print(f"\n📊 By Category:")
    for cat, stats in metrics['by_category'].items():
        success_rate = stats['success@1'] / stats['total'] * 100 if stats['total'] > 0 else 0
        print(f"  {cat.capitalize():15} - Success@1: {stats['success@1']}/{stats['total']} ({success_rate:.1f}%)")
    
    print(f"\n📊 By Difficulty:")
    for diff, stats in metrics['by_difficulty'].items():
        success_rate = stats['success@1'] / stats['total'] * 100 if stats['total'] > 0 else 0
        print(f"  {diff.capitalize():15} - Success@1: {stats['success@1']}/{stats['total']} ({success_rate:.1f}%)")
    
    print("\n" + "="*70)

def save_results(results, metrics, filename='graphrag_results.json'):
    """Save results to JSON file"""
    
    output = {
        'timestamp': datetime.now().isoformat(),
        'system': 'GraphRAG (Hybrid)',
        'configuration': {
            'semantic_weight': 0.7,
            'structural_weight': 0.3,
            'normalization': 'logarithmic'
        },
        'metrics': metrics,
        'detailed_results': results
    }
    
    output_path = os.path.join('data', 'evaluation', filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"✅ Results saved to: {output_path}")

# ============================================================================
# MAIN EVALUATION
# ============================================================================

def main():
    """Run complete evaluation pipeline"""
    
    # Load questions
    questions_path = 'data/evaluation/benchmark_questions.json'
    
    if not os.path.exists(questions_path):
        print(f"❌ ERROR: Questions file not found: {questions_path}")
        print("   Please create the benchmark_questions.json file first")
        return
    
    with open(questions_path, 'r') as f:
        questions = json.load(f)
    
    print(f"📋 Loaded {len(questions)} evaluation questions")
    print(f"   Categories: {set(q['category'] for q in questions)}")
    print(f"   Difficulty levels: {set(q['difficulty'] for q in questions)}")
    
    # Initialize GraphRAG engine
    print("\n🚀 Initializing GraphRAG engine...")
    engine = SmartGraphRAGEngine()
    
    # Run evaluation
    results = evaluate_graphrag(questions, engine)
    
    # Calculate metrics
    metrics = calculate_metrics(results)
    
    # Print report
    print_report(metrics)
    
    # Save results
    save_results(results, metrics)
    
    # Close connections
    engine.close()
    
    print("\n✅ Evaluation complete!")
    
    # Return metrics for further processing
    return metrics, results

if __name__ == "__main__":
    metrics, results = main()