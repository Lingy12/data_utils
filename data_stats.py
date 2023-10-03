import json
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
        sound_arr, sr = entry['audio']['array'], entry['audio']['sampling_rate']
        total_audio_length += len(sound_arr) / sr
    except Exception as e:
        return -1, f'Check index {idx}, {e}', 0
    return 0, 'Success', total_audio_length


def check_data(hf_folder:str, num_worker: int = 4):
    splits = list(filter(lambda x: os.path.isdir(os.path.join(hf_folder, x)), os.listdir(hf_folder)))
    print(splits)
    if len(splits) > 0:
        print('Data containing split')
        target_split = splits
    else:
        target_split = ['']
    stats = {}

    for split in target_split:
        if len(split) != 0:
            print('Checking split {}'.format(split))
        ds = load_from_disk(os.path.join(hf_folder, split))
        N = len(ds)
    
        print(f'total rows = {N}')
        inputs = [(idx, ds) for idx in range(len(ds))]
        ds_audio_length = 0
        with Pool(processes=num_worker) as pool:
            results = list(tqdm(pool.imap(check_entry, inputs), total=N))
        
            for res in results:
                code, message, audio_length = res
            
                if code == -1:
                    print(message)
                else:
                    ds_audio_length += audio_length
            # print(code, message)
            print('Dataset seconds = {}'.format(ds_audio_length))
            print('Dataset minutes = {}'.format(ds_audio_length / 60))
            print('Dataset hours = {}'.format(ds_audio_length / 3600))

            curr_res = {"num_of_row": N, "audio_hours": ds_audio_length / 3600}
            
            if len(split) != 0:
                stats[split] = curr_res
            else:
                stats = curr_res

    # for idx in tqdm(range(len(ds))):
        # try:
            # _ = ds[idx]
        # except:
            # print('Check index = {}'.format(idx))
        print('Dataset checked.')
    
    with open(os.path.join(hf_folder, 'ds_stats.json'), 'w') as f:
        json.dump(stats, f, indent=1)
if __name__ == "__main__":
    fire.Fire(check_data)



