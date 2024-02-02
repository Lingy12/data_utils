# from pydub import AudioSegment
# from IPython.display import Audio
from datasets import load_from_disk, Dataset
# from random import randrange
# import datasets
import numpy as np
import os
from tqdm import tqdm
# import pandas as pd
from typing import List
import fire
def collate_interval(start, end, batch):
    target = batch[start:end]
    # print(target)
    # target = {key: [i[key] for i in target] for key in target[0]}
    # print(target)
    return {
            "utt_id": '+'.join([os.path.basename(id) for id in target['utt_id']]),
            "sentence": '.'.join([s.strip() for s in target['sentence']]),
            "speaker": '+'.join([os.path.join(s) for s in target['speaker']]),
            "audio": 
                {
                    "array": np.concatenate([audio["array"] for audio in target["audio"]]),
                    "sampling_rate": target['audio'][0]['sampling_rate'],
                }
    
    }

def merge_ds(ds_path, sorting_cols:List[str], output_path):
    # ds_path = '/data/geyu/malay/malay_v1.1/ntucs/ntucs_hf/'
    # display(Audio())
    if os.path.exists(output_path):
        flag = input('Path already exists, enter y to overwrite')
        if flag != 'y':
            return 
    sorting_cols = list(sorting_cols)
    print(sorting_cols)
    ds = load_from_disk(ds_path)
    # ds = ds.select(range(30))
    print('data schema: ')
    print(ds)
    def sort_ds(cols, ds):
        return ds.sort(cols)

    ds = sort_ds(sorting_cols, ds)
    print('dataset sorted according to: ' + str(sorting_cols))
    # ds = ds.select(range(30))
    # ds
    res_dict = {"utt_id": [], "sentence": [], "speaker": [], "audio": []}
    curr_acc_time, curr_start= 0, 0 
    for idx in tqdm(range(len(ds))):
        # print(idx)
        entry = ds[idx]
        curr_slice_time = len(entry['audio']['array']) / entry['audio']['sampling_rate']
        # print(curr_acc_time + curr_slice_time)
        if curr_acc_time + curr_slice_time > 30: # maximum 30s
            # print(curr_start, idx)
            collated_interval = collate_interval(curr_start, idx, ds)
            # print(collated_interval)
            for k in res_dict.keys():
                res_dict[k].append(collated_interval[k])
            curr_start = idx
            curr_acc_time = curr_slice_time
        else:
            curr_acc_time += curr_slice_time                       
    combined_ds = Dataset.from_dict(res_dict) 
    # combined_ds = ds.map(mapper_function1, batched=True, batch_size=None, remove_columns=list(ds.features), writer_batch_size=50) # treat full dataset as one batch and perform operation
    print('Length of merged ds = ' + str(len(combined_ds)))

    combined_ds.save_to_disk(output_path)
    # return combined_ds

if __name__ == "__main__":
    fire.Fire(merge_ds)
