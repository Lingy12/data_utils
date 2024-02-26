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
import re
# import pandas as pd
from typing import List
import fire
   

def create_ds(ds_path,  output_path):
    # ds_path = '/data/geyu/malay/malay_v1.1/ntucs/ntucs_hf/'
    # display(Audio())
    if os.path.exists(output_path):
        flag = input('Path already exists, enter y to overwrite')
        if flag != 'y':
            return 
        # combined_ds = ds.map(mapper_function1, batched=True, batch_size=None, remove_columns=list(ds.features), writer_batch_size=50) # treat full dataset as one batch and perform operation
    ds = load_from_disk(ds_path)
    ds.filter(lambda x: '<mandarin>' in x['sentence'])
    
    pattern = r"<mandarin>([\u4e00-\u9fff]+):[a-z\s]+</mandarin>"
    removal_pattern = r"\[.*?\]|\(.*?\)"

    def transform_mandarin(sample):
            text = sample['sentence']
            text = re.sub(pattern, r'\1', text)
            text = re.sub(removal_pattern, '', text)
            sample['sentence'] = text
            return sample


    ds = ds.map(lambda x: transform_mandarin(x))
    ds.save_to_disk(output_path)
    # return combined_ds

if __name__ == "__main__":
    fire.Fire(create_ds)
