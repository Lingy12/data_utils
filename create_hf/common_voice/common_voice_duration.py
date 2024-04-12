import fire
from datasets import DatasetDict, load_from_disk, Dataset, Audio
import multiprocessing as mp
from glob import glob
import os
from tqdm import tqdm
import pandas as pd
import csv

sentence_reported = 'reported.tsv'
splits = ['dev.tsv','other.tsv','test.tsv','train.tsv','invalidated.tsv','validated.tsv']
duration = 'clip_durations.tsv'

def build_language_ds_pd(args):
    language, raw_data_dir = args
    target_folder = os.path.join(raw_data_dir, language)
    duration_file = os.path.join(target_folder, duration)
    # print(target_folder)

    duration_file = pd.read_csv(duration_file, sep="\t", low_memory=False, quoting=csv.QUOTE_NONE)
    return duration_file['duration[ms]'].sum()

def build_hf_ds(raw_data_dir, num_workers = 4):
    language_dir = os.listdir(raw_data_dir)
    params = []

    for lang in language_dir:
        if not os.path.isdir(os.path.join(raw_data_dir, lang)):
            print('Skipping {}'.format(lang))
            continue
        
        params.append((lang, raw_data_dir))
    
    with mp.Pool(num_workers) as p:
        r = list(tqdm(p.imap(build_language_ds_pd, params), total=len(params)))
    
    num_hours = 0
    for res in r:
        hour = res / 3600 / 1000
        num_hours += hour
    print('total_hour = {}'.format(num_hours))

if __name__ == "__main__":
    fire.Fire(build_hf_ds)
