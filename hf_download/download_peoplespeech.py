from datasets import load_dataset
import fire

import os

def download_ds(dest: str, num_worker:int=4):
    print('Downloading and load dataset from hugging face hub...')
    
    print('Using {} processes'.format(num_worker))
    ds = load_dataset('MLCommons/peoples_speech', num_proc=num_worker)
    print('Dataset loaded')

    print('Saving dataset...')
    ds.save_to_disk(dest, num_proc=num_worker)
    print('Dataset saved.')

    ds.cleanup_cache_files()
    print('Local cache cleared')

if __name__ == "__main__":
    fire.Fire(download_ds)
