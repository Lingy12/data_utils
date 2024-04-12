from datasets import load_dataset
import sys
import os

sizes = ['dev', 'test', 'L']
saving_path = sys.argv[1]
workers = sys.argv[2]

os.makedirs(saving_path, exist_ok=True)

for size in sizes:
    saving = os.path.join(saving_path, size)
    ds = load_dataset("kensho/spgispeech", size, use_auth_token=True, num_proc=int(workers), trust_remote_code=True)
    ds.save_to_disk(saving, num_proc=int(workers))

