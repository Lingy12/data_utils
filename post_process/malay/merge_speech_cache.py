# from pydub import AudioSegment
# from IPython.display import Audio
from datasets import Audio, load_from_disk, Dataset
from datasets.features import audio
# from random import randrange
# import datasets
import numpy as np
import os
from tqdm import tqdm
from uuid import uuid4
import soundfile as sf
from pydub import AudioSegment

# import pandas as pd
from typing import List
import fire
def collate_interval(start, end, batch, audio_folder):
    target = batch[start:end]
    # print(target)
    # target = {key: [i[key] for i in target] for key in target[0]}
    # print(target)
    curr_dict = {
            "utt_id": str(start) + '-' + str(end),
            "sentence": '.'.join([s.strip() for s in target['sentence']]),
            "audio": 
                {
                    "array": np.concatenate([audio["array"] for audio in target["audio"]]),
                    "sampling_rate": target['audio'][0]['sampling_rate'],
                }
    
    }

    filename = str(uuid4()) + '.wav'
    audio_path = os.path.join(audio_folder, filename)
    audio_arr, sr = curr_dict["audio"].values()
    # audio = AudioSegment(audio_arr.astype('int16').tobytes(), frame_rate=sr, sample_width=2, channels=1)

    # audio.export(audio_path, format='wav')
    sf.write(audio_path, audio_arr, sr)
    curr_dict['id'] = filename[:-4]
    curr_dict['audio'] = audio_path
    return curr_dict
    

def merge_ds(ds_path, sorting_cols:List[str], output_path, audio_folder):
    # ds_path = '/data/geyu/malay/malay_v1.1/ntucs/ntucs_hf/'
    # display(Audio())
    if not os.path.exists(audio_folder):
        os.makedirs(audio_folder, exist_ok=True)
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
    res_dict = {"utt_id": [], "sentence": [], "audio": []}
    curr_acc_time, curr_start= 0, 0 
    for idx in tqdm(range(len(ds))):
        # print(idx)
        entry = ds[idx]
        curr_slice_time = len(entry['audio']['array']) / entry['audio']['sampling_rate']
        # print(curr_acc_time + curr_slice_time)
        if curr_acc_time + curr_slice_time > 30: # maximum 30s
            # print(curr_start, idx)
            collated_interval = collate_interval(curr_start, idx, ds, audio_folder)
            # print(collated_interval)
            for k in res_dict.keys():
                res_dict[k].append(collated_interval[k])
            curr_start = idx
            curr_acc_time = curr_slice_time
        else:
            curr_acc_time += curr_slice_time                       
    combined_ds = Dataset.from_dict(res_dict) 
    combined_ds = combined_ds.cast_column('audio', Audio())
    # combined_ds = ds.map(mapper_function1, batched=True, batch_size=None, remove_columns=list(ds.features), writer_batch_size=50) # treat full dataset as one batch and perform operation
    print('Length of merged ds = ' + str(len(combined_ds)))

    combined_ds.save_to_disk(output_path)
    # return combined_ds

if __name__ == "__main__":
    fire.Fire(merge_ds)
