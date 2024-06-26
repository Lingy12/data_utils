import json
import sys
from datasets import load_from_disk
import fire
from numpy import array, who
from tqdm import tqdm
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
import os
def check_entry(args):
    idx, ds = args # unpack
    total_audio_length = 0
    try:
        entry = ds[idx]
        sound_arr, sr = entry['context']['audio']['array'], entry['context']['audio']['sampling_rate']
        total_audio_length += len(sound_arr) / sr

        if total_audio_length > 31:
            print(idx)
            print(entry)
            print(total_audio_length)
            return 1, f'Long audio at index {idx}', total_audio_length, len(sound_arr), idx, str(entry),sr 
    except Exception as e:
        return -1, f'Check index {idx}, {e}', 0, None, idx, '', None
    return 0, 'Success', total_audio_length, len(sound_arr), idx, str(entry), sr

def get_all_split(root_hf):
    directories = []
    for dirpath, dirnames, filenames in os.walk(root_hf):
        if len(dirnames) == 0:
            directories.append(dirpath)
    return directories


def check_data(hf_folder:str, num_worker: int = 4):
    splits = list(filter(lambda x: os.path.isdir(os.path.join(hf_folder, x)), os.listdir(hf_folder)))
    splits = get_all_split(hf_folder)
    print(splits)
    print('Total split = {}'.format(len(splits)))
    if len(splits) > 0:
        print('Data containing split')
        target_split = splits
    else:
        target_split = ['']
    stats = {}
    failed_split = []
    for split in target_split:
        if len(split) != 0:
            print('Checking split {}'.format(split))
        try:
            ds = load_from_disk(os.path.join(hf_folder, split))
        except:
            failed_split.append(split)
            print(split + ' failed.')
            continue
        N = len(ds)
    
        print(f'total rows = {N}')
        inputs = [(idx, ds) for idx in range(len(ds))]
        ds_audio_length = 0
        long_audio_length = 0
        max_audio_length = 0
        with Pool(processes=num_worker) as pool:
            results = list(tqdm(pool.imap(check_entry, inputs), total=N))
            
            err_idx = []
            long_idx = {}
            length_map = {}
            for res in results:
                code, message, audio_length, len_audio_arr, index, entry, sr= res
                
                if audio_length > max_audio_length:
                    max_audio_length = audio_length
                if code == -1:
                    err_idx.append(index) #index has error
                    print(message)
                elif code == 1:
                    long_idx[index] = {'entry': entry, 'length': audio_length} #long audio
                    long_audio_length += audio_length
                else:
                    ds_audio_length += audio_length
                    length_map[index] = (len_audio_arr, sr) # only valid audio
            # print(code, message)
            print('Dataset seconds = {}'.format(ds_audio_length))
            print('Dataset minutes = {}'.format(ds_audio_length / 60))
            print('Dataset hours = {}'.format(ds_audio_length / 3600))
            print('Longest video = {}'.format(max_audio_length))
            curr_res = {"num_of_row": N, "audio_hours (valid)": ds_audio_length / 3600, "max_audio_length": max_audio_length, "long_audio_hours": long_audio_length / 3600, "total_audio_hour": ds_audio_length / 3600 + long_audio_length / 3600}
            print(curr_res)
            split_folder = os.path.join(hf_folder, split)
            indexing_file = os.path.join(split_folder, 'indexing.json')
            long_file = os.path.join(split_folder, 'long_audio.json')
            curr_stats = os.path.join(split_folder, 'ds_stats.json')
            with open(indexing_file, 'w') as f:
                json.dump(length_map, f, indent=1)
            with open(long_file, 'w') as f:
                json.dump(long_idx, f, indent=1)
            with open(curr_stats, 'w') as f:
                json.dump(curr_res, f, indent=1)
            if len(err_idx) > 0:
                print('error exists')
                error_file = os.path.join(split_folder, 'err.txt')
                print('check {}'.format(error_file))
                with open(error_file, 'w') as f:
                    for idx in err_idx:
                        f.write(f'{idx}\n')
            if len(split) != 0:
                stats[split] = curr_res
            else:
                stats = curr_res
    
    # for idx in tqdm(range(len(ds))):
        # try:
            # _ = ds[idx]
        # except:
            # print('Check index = {}'.format(idx))
        print(failed_split)
        print('Dataset checked.')
    
    with open(os.path.join(hf_folder, 'ds_stats.json'), 'w') as f:
        json.dump(stats, f, indent=1)
if __name__ == "__main__":
    fire.Fire(check_data)
