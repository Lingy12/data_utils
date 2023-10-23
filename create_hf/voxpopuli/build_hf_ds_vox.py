import fire
from datasets import Audio, Dataset
import os
from glob import glob
from functools import reduce
from tqdm import tqdm
import multiprocessing as mp
schema = ['language', 'year', 'segment_id', 'audio', 'sentence']

def merge_dicts(d1, d2):
    return {k: d1.get(k, []) + d2.get(k, []) for k in set(d1) | set(d2)}

def generate_dict_for_folder(args):
    language, year, root = args
    
    res = {key: [] for key in schema}
    target_path = os.path.join(root, language, year)
    audios = glob(os.path.join(target_path, '*.ogg'))

    for audio in audios:
        segment_id = audio.split('/')[-1][:-4]
        sentence = ''

        for key in res.keys():
            res[key].append(eval(key))
    return res

def build_ds(root_folder, dest, num_workers=4):
    params = []
    languages = os.listdir(root_folder)
    
    for lang in languages:
        years = os.listdir(os.path.join(root_folder, lang))
        for year in years:
            params.append((lang, year, root_folder))


    with mp.Pool(num_workers) as p:
        r = list(tqdm(p.imap(generate_dict_for_folder, params), total=len(params)))

    overall_dict = reduce(merge_dicts, r)
    
    print(len(overall_dict[list(overall_dict.keys())[0]]))
    ds = Dataset.from_dict(overall_dict).cast_column('audio', Audio())

    ds.save_to_disk(dest)
    
    
    
