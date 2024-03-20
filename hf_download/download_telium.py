
import sys
import os
from datasets import load_dataset

output_folder = sys.argv[1]
workers = int(sys.argv[2])

splits = ["release1", "release2", "release3", "release3-speaker-adaptation"]

os.makedirs(output_folder, exist_ok=True)
for split in splits:
    ds = load_dataset("LIUM/tedlium", split, num_proc=workers) # for Release 1   
    output_path = os.path.join(output_folder, split)
    ds.save_to_disk(output_path, num_proc=workers)
    
    