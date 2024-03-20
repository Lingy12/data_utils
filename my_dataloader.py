from datasets import load_from_disk, concatenate_datasets
import fire
import json
import os


def load_json(path):
    with open(path) as f:
        data = json.load(f)
    return data

def filter_long_and_err(ds, ds_dir):
    long_indexing = load_json(os.path.join(ds_dir, 'long_audio.json'))
    long_indices = long_indexing.keys()
    
    if os.path.exists(os.path.join(ds_dir, 'err.txt')):
      with open(os.path.join(ds_dir, 'err.txt')) as f:
            err_indices = f.readlines()
    else:
        err_indices = []
        
    err_indices = list(map(lambda x: int(x), err_indices))
    long_indices = list(map(lambda x: int(x), long_indices))
    filter_indices = long_indices + err_indices
    ds = ds.select([i for i in range(len(ds)) if i not in filter_indices])
    return ds

class MyHFLoader:
    
    def __init__(self, filter: bool):
        self.filter = filter
    
    def recursive_fault_tolerent_load(self, dataset_root):
        ds_lst = []
        for root, dirs, files in os.walk(dataset_root):
            if len(dirs) == 0:
                split_ds_path = root # only consider last level directory
                split_path = os.path.relpath(root, dataset_root)
                split_ds = load_from_disk(split_ds_path)
                split_ds = split_ds.add_column(name='split_name', column=[split_path] * len(split_ds))
                split_ds = split_ds.add_column(name='index', column=list(range(0, len(split_ds))))
                
                if self.filter:
                    split_ds = filter_long_and_err(split_ds, split_ds_path)
                ds_lst.append(split_ds)

        ds = concatenate_datasets(ds_lst)
        
        print('Loaded {} samples'.format(len(ds)))
        return ds

        
