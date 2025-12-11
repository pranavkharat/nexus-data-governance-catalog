# scripts/create_performance_labels.py

"""
Generate PERFORMANCE-BASED training labels
Tests all 4 routes on each question, labels with best performer
This is the CORRECT way to train XGBoost!
"""

import json
import sys
import os
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

from src.graphrag.smart_graphrag_engine import SmartGraphRAGEngine
from src.graphrag.query_features import QueryFeatureExtractor

QUESTIONS_PATH = os.path.join(PROJECT_ROOT, 'data', 'evaluation', 'benchmark_questions.json')
TRAINING_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'training', 'performance_based_labels.json')

def normalize_table(name):
    """Normalize table names for comparison"""
    if not name:
        return ""
    parts = name.upper().split('.')
    return '.'.join(parts[-2:]) if len(parts) >= 2 else name.upper()

def is_correct(result_table, question):
    """Check if result matches ground truth"""
    if not result_table:
        return False
    
    result_norm = normalize_table(result_table)
    
    # Check ground truth
    for gt in question['ground_truth']:
        gt_norm = normalize_table(gt)
        if gt_norm in result_norm or result_norm in gt_norm:
            return True
    
    # Check acceptable
    for acc in question.get('acceptable', []):
        acc_norm = normalize_table(acc)
        if acc_norm in result_norm or result_norm in acc_norm:
            return True
    
    return False

def test_route(engine, question, route_name):
    """
    Test one specific route on a question
    Returns: (correct: bool, top_result: str)
    """
    
    q_text = question['question']
    
    try:
        # Force execution of specific route
        if route_name == 'semantic_discovery':
            results = engine._hybrid_search_query(q_text, top_k=5)
        elif route_name == 'metadata_filter':
            results = engine._metadata_filter_query(q_text, top_k=5)
        elif route_name == 'duplicate_detection':
            results = engine._duplicate_query(q_text, top_k=5)
        elif route_name == 'relationship_traversal':
            results = engine._relationship_query(q_text, top_k=5)
        else:
            return False, None
        
        # Check if results exist
        if not results:
            return False, None
        
        # Get top result
        top_result = results[0].get('table')
        
        # Check if correct
        correct = is_correct(top_result, question)
        
        return correct, top_result
        
    except Exception as e:
        print(f"    ⚠️  Route {route_name} failed: {e}")
        return False, None

def generate_performance_labels():
    """
    For each question:
    1. Test all 4 routes
    2. Record which ones got correct answer
    3. Choose best route as label
    """
    
    print("🎯 Generating Performance-Based Training Labels")
    print("=" * 70)
    print("This will test all 4 routes on all 60 questions")
    print("Expected time: 5-10 minutes")
    print("=" * 70)
    
    # Load questions
    with open(QUESTIONS_PATH, 'r') as f:
        questions = json.load(f)
    
    print(f"\n✅ Loaded {len(questions)} questions")
    
    # Initialize engine
    print("\n🔌 Initializing SmartGraphRAG engine...")
    engine = SmartGraphRAGEngine()
    
    # Initialize feature extractor
    extractor = QueryFeatureExtractor()
    
    # Test all routes
    training_data = []
    route_names = ['semantic_discovery', 'metadata_filter', 'duplicate_detection', 'relationship_traversal']
    
    # Statistics
    route_success_count = {r: 0 for r in route_names}
    no_route_works = 0
    multiple_routes_work = 0
    
    for i, question in enumerate(questions, 1):
        q_text = question['question']
        q_id = question['id']
        
        print(f"\n[{i}/{len(questions)}] Q{q_id}: {q_text}")
        
        # Test all 4 routes
        route_results = {}
        for route in route_names:
            correct, top_result = test_route(engine, question, route)
            route_results[route] = {
                'correct': correct,
                'top_result': top_result
            }
            
            status = "✅" if correct else "❌"
            print(f"   {status} {route}: {top_result if top_result else 'No results'}")
        
        # Determine best route(s)
        correct_routes = [r for r, res in route_results.items() if res['correct']]
        
        if len(correct_routes) == 0:
            # No route got it right - use semantic as default
            best_route = 'semantic_discovery'
            no_route_works += 1
            print(f"   ⚠️  NO ROUTE CORRECT - Defaulting to semantic_discovery")
            
        elif len(correct_routes) == 1:
            # One route worked - use it!
            best_route = correct_routes[0]
            route_success_count[best_route] += 1
            print(f"   ✅ BEST ROUTE: {best_route}")
            
        else:
            # Multiple routes work - choose by priority
            # Priority: duplicate > relationship > metadata > semantic
            # (more specific routes preferred)
            priority = ['duplicate_detection', 'relationship_traversal', 
                       'metadata_filter', 'semantic_discovery']
            
            best_route = next((r for r in priority if r in correct_routes), correct_routes[0])
            multiple_routes_work += 1
            route_success_count[best_route] += 1
            
            print(f"   ⚠️  Multiple routes correct: {correct_routes}")
            print(f"   → Choosing: {best_route} (highest priority)")
        
        # Extract features
        features = extractor.extract_features(q_text)
        
        # Save training example
        training_data.append({
            'question_id': q_id,
            'question': q_text,
            'category': question.get('category'),
            'features': features,
            'best_route': best_route,  # PERFORMANCE-BASED LABEL!
            'all_routes_tested': route_results,
            'correct_routes': correct_routes
        })
    
    # Save training data
    os.makedirs(os.path.dirname(TRAINING_DATA_PATH), exist_ok=True)
    
    with open(TRAINING_DATA_PATH, 'w') as f:
        json.dump(training_data, f, indent=2)
    
    print("\n" + "=" * 70)
    print("✅ PERFORMANCE-BASED LABELS GENERATED")
    print("=" * 70)
    print(f"💾 Saved to: {TRAINING_DATA_PATH}")
    
    print(f"\n📊 Statistics:")
    print(f"   Total questions: {len(training_data)}")
    print(f"   No route worked: {no_route_works} ({no_route_works/len(questions)*100:.1f}%)")
    print(f"   Multiple routes worked: {multiple_routes_work} ({multiple_routes_work/len(questions)*100:.1f}%)")
    
    print(f"\n📈 Route Performance (how often each route got correct answer):")
    for route, count in sorted(route_success_count.items(), key=lambda x: x[1], reverse=True):
        print(f"   {route}: {count}/{len(questions)} ({count/len(questions)*100:.1f}%)")
    
    print(f"\n📋 Final Label Distribution (what XGBoost will learn):")
    from collections import Counter
    label_dist = Counter([ex['best_route'] for ex in training_data])
    for route, count in label_dist.most_common():
        print(f"   {route}: {count} ({count/len(training_data)*100:.1f}%)")
    
    engine.close()
    
    return training_data

if __name__ == "__main__":
    generate_performance_labels()