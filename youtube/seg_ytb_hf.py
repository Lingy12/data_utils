import fire
from datasets import load_from_disk, Dataset, Audio, Features, Value
from dataclasses import dataclass
from typing import List
import math
import os
import numpy as np

def split_audio(raw_audio, sampling_rate):
    raw_array = raw_audio['array']
    total_segment = math.ceil(len(raw_array) / (sampling_rate * 30.0))
    step = sampling_rate * 30
    n = len(raw_array)
    # print(raw_array)
    return [{"array": raw_array[i * step: min((i + 1) * step, n)], "sampling_rate": sampling_rate} for i in range(total_segment)]

# split for unsupervised dataset
@dataclass
class SegmenteAudio(object):
    sampling_rate: int 

    def __call__(self, batch):
        output_batch = {k : [] for k in set(batch.keys()).union(set(["segment_id"]))}
        
        audios = batch['audio']
        keep_keys = set(output_batch.keys()) - set(['audio', "segment_id"]) 
        
        for i in range(len(audios)):
            audio = audios[i]
            curr_split = split_audio(audio, self.sampling_rate)
            output_batch['audio'].extend(curr_split)
            output_batch['segment_id'].extend(list(range(len(curr_split))))
            for k in keep_keys:
                output_batch[k].extend([batch[k][i]] * len(curr_split))
        # print(output_batch)
        return output_batch
    
    
def segment_ytb_to_hf(dataset_folder, output_path, batch_size=10, workers=4):
 
    ori_ds = load_from_disk(dataset_folder)
    ori_ds = ori_ds.cast_column('audio', Audio(sampling_rate=16000))
    split_funct = SegmenteAudio(sampling_rate=16000)
    
    # print(ori_ds)
    features_dict = {k: ori_ds.features[k] for k in ori_ds.column_names}
    features_dict.update({"segment_id": Value(dtype='int8')})
    print(features_dict)
    features = Features(features_dict)
    splited_ds = ori_ds.map(split_funct, batched=True, num_proc=workers, batch_size=batch_size, 
                remove_columns=ori_ds.column_names, features=features)
    
    splited_ds.save_to_disk(output_path)

if __name__ == "__main__":
    fire.Fire(segment_ytb_to_hf)
