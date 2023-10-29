import json
import multiprocessing
from datasets import load_from_disk
import fire
from numpy import array, who
from tqdm import tqdm
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
import os
import datasets
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

def get_all_split(root_hf):
    directories = []
    for dirpath, dirnames, filenames in os.walk(root_hf):
        if len(dirnames) == 0:
            directories.append(dirpath)
    return directories

def get_split_rank(i, world_size, splits):
    batch_size = int(len(splits) / world_size)
    start = i * batch_size
    end = (i + 1) * batch_size
    
    if i == world_size - 1:
        return splits[start:]
    else:
        return splits[start:end]

def check_split_rank(rank, world_size, splits, hf_folder, worker_assigned, queue):
    prefix = '# rank {}: '.format(rank)
    print(prefix + 'assigned worker = {}'.format(worker_assigned))
    target_split = get_split_rank(rank, world_size, splits)
    for split in target_split:
        if len(split) != 0:
            print(prefix + 'Checking split {}'.format(split))
        try:
            ds = load_from_disk(os.path.join(hf_folder, split))
        except:
            # raise Exception('Dataset reading failed.')
            queue.put((rank, 0,split,None))
            continue
        N = len(ds)
        split_path = os.path.join(hf_folder, split)
        print(prefix + f'total rows = {N}')
        inputs = [(idx, ds) for idx in range(len(ds))]
        ds_audio_length = 0
        split_data = os.path.join(split_path, 'split_stats.json')
        if os.path.exists(split_data):
            with open(split_data, 'r') as f:
                curr_res = json.load(f)
            ds_audio_length = curr_res['audio_hours'] * 3600
            print(prefix + '{} data exists, skip checking.'.format(split))
        else:
            with Pool(processes=worker_assigned) as pool:
                results = list(tqdm(pool.imap(check_entry, inputs), total=N, desc=prefix + ' ' + split))
        
            for res in results:
                code, message, audio_length = res
            
                if code == -1:
                    print(message)
                else:
                    ds_audio_length += audio_length
            # print(code, message)
        print(prefix + split)
        print(prefix + 'Dataset seconds = {}'.format(ds_audio_length))
        print(prefix + 'Dataset minutes = {}'.format(ds_audio_length / 60))
        print(prefix + 'Dataset hours = {}'.format(ds_audio_length / 3600))

        curr_res = {"num_of_row": N, "audio_hours": ds_audio_length / 3600}

        with open(os.path.join(split_path, 'split_stats.json'), 'w') as f:
            json.dump(curr_res, indent=1, fp=f)
        queue.put((rank, 1, split, curr_res))

    
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
    
    processes = []
    result_queue = multiprocessing.Queue()
    if num_worker < len(splits):
        worker_assigned = 1
    else:
        worker_assigned = int(num_worker / len(target_split))
        num_worker = int(num_worker / worker_assigned)
    for i in range(num_worker):
        p = multiprocessing.Process(target=check_split_rank, args=(i, num_worker, target_split, hf_folder, worker_assigned, result_queue))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    for res in results:
        _, status, split, curr_res = res
        if status == 0:
            failed_split.append(split)
            continue

        if len(split) != 0:
            stats[split] = curr_res
        else:
            stats = curr_res

    print(failed_split)
    print('Dataset checked.')
    
    with open(os.path.join(hf_folder, 'ds_stats.json'), 'w') as f:
        json.dump(stats, f, indent=1)
if __name__ == "__main__":
    fire.Fire(check_data)
