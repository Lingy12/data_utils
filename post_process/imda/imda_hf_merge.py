from typing import List
from datasets import Audio, load_from_disk, Features, Value
import fire
from dataclasses import dataclass
import os
import numpy as np
import random

from numpy.core.multiarray import dtype

def collate_interval(audio_lst, sampling_rate):
    # print(audio_lst)
    return {"array":np.concatenate([audio for audio in audio_lst]),
                "sampling_rate": sampling_rate}
    
@dataclass
class MergeTransform(object):
    sorting_cols: List[str]
    sampling_rate: int
    enable_random_start: bool

    def __call__(self, batch):
        output_batch = {k: [] for k in batch.keys()}
        output_batch['audio_length'] = []
    
        audio_lst, text_lst = [], []
        curr_acc_time = 0
        meta_data = {k: [] for k in self.sorting_cols}
        audios = batch['audio']
        indices = list(range(len(audios)))

        if self.enable_random_start:
            rand_rotation_start = random.randint(0, len(indices) - 1)

            indices = indices[rand_rotation_start:] + indices[:rand_rotation_start]
        # print(indices)
        for idx in indices:
            audio = audios[idx]
            text = batch['sentence'][idx]
            curr_slice_time = len(audio['array']) / self.sampling_rate
            # print(curr_acc_time)
            if curr_acc_time + curr_slice_time >= 30 and len(audio_lst) > 0:
                collated_audio = collate_interval(audio_lst, sampling_rate=self.sampling_rate)
                output_batch['audio'].append(collated_audio)
                output_batch['sentence'].append('.'.join([s.strip() for s in text_lst]))
                output_batch['audio_length'].append(len(collated_audio['array']) / self.sampling_rate)
                
                for k in self.sorting_cols:
                    meta_data_unique = list(set(meta_data[k]))
                    output_batch[k].append('-'.join(meta_data_unique))
                    meta_data[k] = [batch[k][idx]]
                curr_acc_time = curr_slice_time
                audio_lst = [audio['array']]
                text_lst = [text] 
            else: 
                curr_acc_time += curr_slice_time
                audio_lst.append(audio['array'])
                text_lst.append(text)
                for k in self.sorting_cols:
                    meta_data[k].append(batch[k][idx])
        # print(len(audio_lst))
        # print(audio_lst)
        if len(audio_lst) > 0:
            collated_audio = collate_interval(audio_lst, sampling_rate=self.sampling_rate)
            output_batch['audio'].append(collated_audio)
            output_batch['sentence'].append('.'.join([s.strip() for s in text_lst]))
            output_batch['audio_length'].append(len(collated_audio['array']) / self.sampling_rate)
            for k in self.sorting_cols:
                meta_data_unique = list(set(meta_data[k]))
                output_batch[k].append('-'.join(meta_data_unique))
 
        # print(output_batch)
        return output_batch
# def merge_batch(batch):
    # print(batch)
    

    # return batch


def merge_ds(input, sorting_cols:List[str], output_path, batch_size=100, workers=4):

    if os.path.exists(output_path):
        flag = input('Path already exists, enter y to overwrite')
        if flag != 'y':
            return

    ds = load_from_disk(input)
    
    sorting_cols = list(sorting_cols)
    print(sorting_cols)
    # ds = ds.select(range(30))
    print('data schema: ')
    print(ds)
    def sort_ds(cols, ds):
        return ds.sort(cols)
    columns_to_keep = sorting_cols + ['sentence', 'audio']
    ds = ds.select_columns(columns_to_keep)
    print(ds)
    ds = sort_ds(sorting_cols, ds)
    ds = ds.cast_column('audio', Audio(sampling_rate=16000))
    merger_funct = MergeTransform(sorting_cols=sorting_cols, sampling_rate=16000, enable_random_start=True)
    features_dict = {k: ds.features[k] for k in sorting_cols}
    features_dict.update({"audio": Audio(sampling_rate=16000, decode=True), "sentence": Value(dtype='string'), "audio_length": Value(dtype='float')})
    print(features_dict)
    # exit()
    features = Features(features_dict)
    ds = ds.map(merger_funct, batched=True, num_proc=workers, batch_size=batch_size, 
                remove_columns=ds.column_names, features=features)

    ds.save_to_disk(output_path)

if __name__ == "__main__":
    fire.Fire(merge_ds)
