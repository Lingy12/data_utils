from datasets import load_from_disk
import fire
from numpy import array, who
from tqdm import tqdm
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

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
    ds = load_from_disk(hf_folder)
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
    # for idx in tqdm(range(len(ds))):
        # try:
            # _ = ds[idx]
        # except:
            # print('Check index = {}'.format(idx))
    print('Dataset checked.')

if __name__ == "__main__":
    fire.Fire(check_data)



