import json
import os
from datasets import load_from_disk
import fire
from multiprocessing import Pool
from tqdm import tqdm

def check_entry(args):
    ds, start, end = args  # Includes dataset, start and end indices for chunk
    results = []
    for idx in range(start, end):
        total_audio_length = 0
        try:
            entry = ds[idx]
            sound_arr, sr = entry['audio']['array'], entry['audio']['sampling_rate']
            total_audio_length += len(sound_arr) / sr
            if total_audio_length > 31:
                results.append((1, f'Long audio at index {idx}', total_audio_length, len(sound_arr), idx, str(entry), sr))
        except Exception as e:
            results.append((-1, f'Check index {idx}, {e}', 0, None, idx, '', None))
        else:
            results.append((0, 'Success', total_audio_length, len(sound_arr), idx, str(entry), sr))
    return results

def get_all_split(root_hf):
    directories = []
    for dirpath, dirnames, filenames in os.walk(root_hf):
        if len(dirnames) == 0:
            directories.append(dirpath)
    return directories

def check_data(hf_folder: str, num_worker: int = 4):
    splits = list(filter(lambda x: os.path.isdir(os.path.join(hf_folder, x)), os.listdir(hf_folder)))
    splits = get_all_split(hf_folder)
    print(splits)
    print('Total split = {}'.format(len(splits)))
    target_split = splits if len(splits) > 0 else ['']
    stats = {}
    failed_split = []
    
    for split in target_split:
        print('Checking split {}'.format(split))
        try:
            ds = load_from_disk(os.path.join(hf_folder, split))
        except:
            failed_split.append(split)
            print(split + ' failed.')
            continue
        
        N = len(ds)
        chunk_size = (N + num_worker - 1) // num_worker  # Ensures evenly distributed workload
        pool_inputs = [(ds, i, min(i + chunk_size, N)) for i in range(0, N, chunk_size)]
        
        with Pool(processes=num_worker) as pool:
            results = list(tqdm(pool.imap(check_entry, pool_inputs), total=len(pool_inputs)))
            # Flatten results list
            flat_results = [item for sublist in results for item in sublist]
        
        process_results(flat_results, ds, split, hf_folder)

def process_results(results, ds, split, hf_folder):
    ds_audio_length, long_audio_length, max_audio_length = 0, 0, 0
    err_idx, long_idx, length_map = [], {}, {}
    for res in results:
        code, message, audio_length, len_audio_arr, index, entry, sr = res
        if audio_length > max_audio_length:
            max_audio_length = audio_length
        if code == -1:
            err_idx.append(index)
        elif code == 1:
            long_idx[index] = {'entry': entry, 'length': audio_length}
            long_audio_length += audio_length
        else:
            ds_audio_length += audio_length
            length_map[index] = (len_audio_arr, sr)

    # Outputs for tracking and debugging
    print(f'Dataset seconds = {ds_audio_length}')
    print(f'Dataset minutes = {ds_audio_length / 60}')
    print(f'Dataset hours = {ds_audio_length / 3600}')
    print(f'Longest audio = {max_audio_length}')
    print(f'Errors at indices: {err_idx}')

    save_results(split, hf_folder, err_idx, long_idx, length_map, ds_audio_length, long_audio_length, max_audio_length)

def save_results(split, hf_folder, err_idx, long_idx, length_map, ds_audio_length, long_audio_length, max_audio_length):
    split_folder = os.path.join(hf_folder, split)
    indexing_file = os.path.join(split_folder, 'indexing.json')
    long_file = os.path.join(split_folder, 'long_audio.json')
    curr_stats = os.path.join(split_folder, 'ds_stats.json')
    curr_res = {"num_of_row": len(length_map), "audio_hours (valid)": ds_audio_length / 3600, "max_audio_length": max_audio_length, "long_audio_hours": long_audio_length / 3600, "total_audio_hour": ds_audio_length / 3600 + long_audio_length / 3600}
    
    with open(indexing_file, 'w') as f:
        json.dump(length_map, f, indent=1)
    with open(long_file, 'w') as f:
        json.dump(long_idx, f, indent=1)
    with open(curr_stats, 'w') as f:
        json.dump(curr_res, f, indent=1)
    if err_idx:
        error_file = os.path.join(split_folder, 'err.txt')
        with open(error_file, 'w') as f:
            for idx in err_idx:
                f.write(f'{idx}\n')

if __name__ == "__main__":
    fire.Fire(check_data)
