import json

with open('data/evaluation/comparative_results.json', 'r') as f:
    results = json.load(f)

for system_name, system_data in results['metrics'].items():
    print(f"{system_name}:")
    print(f"  Success@1: {system_data['success@1_count']}/{system_data['total_questions']}")
    print(f"  Rate: {system_data['success@1_rate']*100:.1f}%")
    print()