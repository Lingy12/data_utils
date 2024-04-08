import multiprocessing
from itertools import chain
from functools import partial
import random
from datasets import load_from_disk, Dataset, Audio
from typing import List
import os
from multiprocess.pool import worker
from tqdm import tqdm
from uuid import uuid4
import numpy as np
import soundfile as sf
import fire

def collate_interval(start, end, batch, audio_folder):
    target = batch[start:end]
    curr_dict = {
        "utt_id": str(start) + '-' + str(end),
        "sentence": '.'.join([s.strip() for s in target['sentence']]),
        "audio": {
            "array": np.concatenate([audio["array"] for audio in target["audio"]]),
            "sampling_rate": target['audio'][0]['sampling_rate'],
        }
    }

    filename = str(uuid4()) + '.wav'
    audio_path = os.path.join(audio_folder, filename)
    audio_arr, sr = curr_dict["audio"].values()
    sf.write(audio_path, audio_arr, sr)
    curr_dict['id'] = filename[:-4]
    curr_dict['audio'] = audio_path
    return curr_dict

def process_group(group_key, ds, grouping_cols, audio_folder):
    res_dict = {"utt_id": [], "sentence": [], "audio": []}
    group_ds = ds.filter(lambda example: all(example[col] == val for col, val in zip(grouping_cols, group_key)))
    group_len = len(group_ds)
    start_idx = random.randint(0, group_len - 1)
    curr_acc_time, curr_start = 0, 0

    indices = list(range(start_idx, group_len)) + list(range(start_idx))
    group_ds = group_ds.select(indices)
    for idx in range(len(group_ds)):
        entry = group_ds[idx]
        curr_slice_time = len(entry['audio']['array']) / entry['audio']['sampling_rate']
        if curr_acc_time + curr_slice_time > 30:
            collated_interval = collate_interval(curr_start, idx, group_ds, audio_folder)
            for k in res_dict.keys():
                res_dict[k].append(collated_interval[k])
            curr_start = idx
            curr_acc_time = curr_slice_time
        else:
            curr_acc_time += curr_slice_time
    return res_dict

def find_unique_groups(args):
    idx, grouping_cols, ds = args
    # chunk = ds[chunk[0]:chunk[1]]
    # example = ds[idx]
    return set(tuple(ds[idx][col] for col in grouping_cols))

def merge_ds(ds_path, sorting_cols: List[str], grouping_cols: List[str], output_path, audio_folder, workers=16):
    if not os.path.exists(audio_folder):
        os.makedirs(audio_folder, exist_ok=True)
    if os.path.exists(output_path):
        flag = input('Path already exists, enter y to overwrite: ')
        if flag.lower() != 'y':
            print("Operation cancelled.")
            return

    print("Sorting columns: ", sorting_cols)
    ds = load_from_disk(ds_path)
    print('Data schema: ', ds)

    ds = ds.sort(list(sorting_cols))
    print('Dataset sorted according to: ', sorting_cols)
    print(grouping_cols)
    # Create unique composite keys for groups
    unique_groups = set(tuple(example[col] for col in grouping_cols) for example in tqdm(ds))
    # print('group generated.')

    # chunk_size = len(ds) // workers
    # chunks = [(i, i + chunk_size) for i in range(0, len(ds), chunk_size)]
    
    # params = [(idx, grouping_cols, ds) for idx in range(len(ds))]
    # Use multiprocessing to find unique groups in each chunk
    # print('finding unique groups')
    # with multiprocessing.Pool(processes=workers) as pool:
        # unique_groups_chunks = list(tqdm(pool.imap_unordered(find_unique_groups, params), total=len(params), desc="Finding unique groups"))

    # Combine unique groups from all chunks
    # unique_groups = set(chain.from_iterable(unique_groups_chunks))
    # unique_groups = set([(example[col] for col in grouping_cols) for example in tqdm(ds)])
    with multiprocessing.Pool(processes=workers) as pool:
        process_partial = partial(process_group, ds=ds, grouping_cols=grouping_cols, audio_folder=audio_folder)
        tasks = pool.imap_unordered(process_partial, unique_groups)
        results = list(tqdm(tasks, total=len(unique_groups), desc="Processing groups"))

    combined_res_dict = {"utt_id": [], "sentence": [], "audio": []}
    for res_dict in results:
        for k in combined_res_dict.keys():
            combined_res_dict[k].extend(res_dict[k])

    combined_ds = Dataset.from_dict(combined_res_dict)
    combined_ds = combined_ds.cast_column('audio', Audio())
    print('Length of merged ds = ', len(combined_ds))

    combined_ds.save_to_disk(output_path)

if __name__ == "__main__":
    fire.Fire(merge_ds)

