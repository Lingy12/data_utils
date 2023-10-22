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


def build_language_ds_pd(args):
    language, raw_data_dir = args
    target_folder = os.path.join(raw_data_dir, language)
    clips_directory = os.path.join(target_folder, 'clips')
    dfs = []
    status = 1
    # print(target_folder)
    for split in splits:
        # print(split)
        split_path = os.path.join(target_folder, split)
        if not os.path.exists(split_path):
            print('{} not exists for {}'.format(split, language))
            continue
        # print(split_path)
        try:
            split_data = pd.read_csv(split_path, sep="\t", low_memory=False, quoting=csv.QUOTE_NONE)
        except Exception as e:
            print(e)
            print('Failed to process {}'.format(split_path))
            status = 0
            continue
        split_data['split'] = split[:-4] # append split
        split_data['audio'] = split_data['path'].map(lambda x: os.path.join(clips_directory, x))
        split_data['language'] = language    
        dfs.append(split_data.copy())
    return pd.concat(dfs), status

def build_hf_ds(raw_data_dir, dest, num_workers = 4):
    language_dir = os.listdir(raw_data_dir)
    params = []

    for lang in language_dir:
        if not os.path.isdir(os.path.join(raw_data_dir, lang)):
            print('Skipping {}'.format(lang))
            continue
        
        params.append((lang, raw_data_dir))
    
    with mp.Pool(num_workers) as p:
        r = list(tqdm(p.imap(build_language_ds_pd, params), total=len(params)))
    
    df_lst = []
    for res in r:
        df, status = res
        if status == 0:
            print('Please check tsv first.')
            exit()
        df_lst.append(df)
    overall_df = pd.concat(df_lst)
    print('Total rows = {}'.format(len(overall_df)))
    langs = overall_df['language'].unique()
    ds_dict = DatasetDict()
    for lang in langs:
        ds = Dataset.from_pandas(overall_df[overall_df['language'] == lang], preserve_index=False).cast_column('audio', Audio())
        ds_dict[lang] = ds
    ds_dict.save_to_disk(dest, num_proc=num_workers)

if __name__ == "__main__":
    fire.Fire(build_hf_ds)
