import fire
from datasets import load_from_disk, Dataset, Audio, Features
from dataclasses import dataclass
from typing import List
import math
import os

def split_audio(raw_array, sampling_rate):
    total_segment = math.ceil(len(raw_array) / (sampling_rate * 30.0))
    step = sampling_rate * 30.0
    n = len(raw_array)
    return [{"audio": raw_array[i * step: min((i + 1) * step, n)], "sampling_rate": sampling_rate} for i in range(total_segment)]

# split for unsupervised dataset
@dataclass
class SegmenteAudio(object):
    sampling_rate: int 

    def __call__(self, batch):
        output_batch = {k : [] for k in batch.keys() + "segment_id"}
        
        audios = batch['audio']
        keep_keys = output_batch.keys() - set(['audio', "segment_id"]) 
        
        for i in range(len(audios)):
            audio = audios[i]
            curr_split = split_audio(audio, self.sampling_rate)
            output_batch['audio'].extend(curr_split)
            output_batch['segment_id'].extend(list(range(len(curr_split))))
            for k in keep_keys:
                output_batch[k].extend([batch[k][i]] * len(curr_split))
        return output_batch
    
    
def segment_ytb_to_hf(raw_folder, output_path, batch_size=10, workers=4):
    
    ori_dict = {"audio": [], "parent": [], "sentence": [], "ytb_id": []}
    
        # Iterate through all files in the raw_folder directory
    for root, dirs, files in os.walk(raw_folder):
        for file in files:
            if file.endswith(".mp3"):
                # Construct the full path of the file
                file_path = os.path.join(root, file)
                # Extract the base directory name and the file name
                base_dir = os.path.basename(os.path.dirname(file_path))
                file_name = os.path.basename(file_path)

                # Extract the YouTube ID (filename without .mp3)
                ytb_id = os.path.splitext(file_name)[0]
                
                # Append the data to the dictionary
                ori_dict["audio"].append(file_name)
                ori_dict["parent"].append(base_dir)
                ori_dict["sentence"].append("")  # Assuming 'sentence' to be filled later or elsewhere
                ori_dict["ytb_id"].append(ytb_id)
    
    
    ori_ds = Dataset.from_dict(ori_dict)
    ori_ds = ori_ds.cast_column('audio', Audio(sampling_rate=16000))
    split_funct = SegmenteAudio(sampling_rate=16000)
    
    features_dict = {k: ori_ds.features[k] for k in ori_ds}
    features = Features(features_dict)
    
    splited_ds = ori_ds.map(split_funct, batched=True, num_proc=workers, batch_size=batch_size, 
                remove_columns=ori_ds.column_names, features=features)
    
    splited_ds.save_to_disk(output_path)