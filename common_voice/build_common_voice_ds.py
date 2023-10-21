import fire
from datasets import load_from_disk, Dataset, Audio
import multiprocessing as mp
from glob import glob
import os
from tqdm import tqdm
import pandas as pd

sentence_reported = 'reported.tsv'
splits = ['dev.tsv','other.tsv','test.tsv','train.tsv','invalidated.tsv','validated.tsv']


def build_language_ds_pd(args):
    language, raw_data_dir = args
    target_folder = os.path.join(raw_data_dir, language)
    clips_directory = os.path.join(target_folder, 'clips')
    dfs = []
    # print(target_folder)
    for split in splits:
        # print(split)
        split_path = os.path.join(target_folder, split)
        if not os.path.exists(split_path):
            print('{} not exists for {}'.format(split, language))
            continue
        # print(split_path)
        split_data = pd.read_csv(split_path, sep="\t")
        split_data['split'] = split[:-4] # append split
        split_data['audio'] = split_data['path'].map(lambda x: os.path.join(clips_directory, x))
        split_data['language'] = language    
        dfs.append(split_data.copy())
    return pd.concat(dfs)

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
    overall_df = pd.concat(r)
    ds = Dataset.from_pandas(overall_df, preserve_index=False)
    ds = ds.cast_column('audio', Audio())
    ds.save_to_disk(dest, num_proc=num_workers)

if __name__ == "__main__":
    fire.Fire(build_hf_ds)
