import os
import json
import fire

def aggregate_counts(directory):
    aggregate_results = {'success_count': 0, 'fail_count': 0}

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.startswith("count_") and file.endswith(".json"):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r') as f:
                        counts = json.load(f)
                        aggregate_results['success_count'] += counts.get('success_count', 0)
                        aggregate_results['fail_count'] += counts.get('fail_count', 0)
                except Exception as e:
                    print(f"Error reading {filepath}: {e}")

    return aggregate_results

if __name__ == '__main__':
    fire.Fire(aggregate_counts)
