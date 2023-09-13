from datasets import load_dataset
import fire

# langs = ['en', 'de', 'fr', 'es', 'pl', 'it', 'ro', 
 # 'hu', 'cs', 'nl', 'fi', 'hr', 'sk', 'sl', 'et', 'lt', 'en_accented']

def download_ds(lang: str, dest: str, num_worker:int = 4):
    print('Downloading and load dataset from hugging face hub...')
    ds = load_dataset('facebook/voxpopuli', lang, num_proc=num_worker)
    print('Dataset loaded')

    print('Saving dataset...')
    ds.save_to_disk(dest, num_proc=num_worker)
    print('Dataset saved.')

if __name__ == "__main__":
    fire.Fire(download_ds)
