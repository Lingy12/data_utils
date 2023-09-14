from datasets import load_dataset
import fire
import os

langs = ['et', 'en', 'de', 'fr', 'es', 'pl', 'it', 'ro', 
 'hu', 'cs', 'nl', 'fi', 'hr', 'sk', 'sl',  'lt', 'en_accented']

def download_ds(dest: str, num_worker:int = 4):
    for lang in langs:
        curr_dest = os.path.join(dest, lang)
        
        if os.path.exists(curr_dest):
            print(f'{lang} ds downloaded, skipping')
            continue
        print(f'Downloading and load {lang} dataset from hugging face hub...')
        ds = load_dataset('facebook/voxpopuli', lang, num_proc=num_worker)
        print('Dataset loaded')
        print(f'Saving dataset to {curr_dest}')
        ds.save_to_disk(curr_dest, num_proc=num_worker)
        print('Dataset saved.')

if __name__ == "__main__":
    fire.Fire(download_ds)
