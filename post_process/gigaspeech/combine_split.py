from datasets import load_from_disk, concatenate_datasets
from typing import List
import os
import fire

def combine_split(folder, splits: List[str], output_dir):
        to_combine = []
        print(splits)
        for split in splits:
            split_ds = load_from_disk(os.path.join(folder, split))
            split_ds = split_ds.add_column('split',[split] * len(split_ds))
            print(split_ds)
            to_combine.append(split_ds)
        ds = concatenate_datasets(to_combine)
        ds.save_to_disk(output_dir)

if __name__ == '__main__':
    fire.Fire(combine_split)
