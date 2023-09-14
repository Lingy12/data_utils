from datasets import load_dataset
import fire
def download_ds(ds_hf_name:str, dest: str, num_worker:int = 4):
    print('Downloading and load dataset from hugging face hub...')
    ds = load_dataset(ds_hf_name, num_proc=num_worker, cache_dir=dest)
    print('Dataset loaded')

    print('Saving dataset...')
    ds.save_to_disk(dest, num_proc=num_worker)
    print('Dataset saved.')

    ds.cleanup_cache_files()
    print('Local cache cleared')

if __name__ == "__main__":
    fire.Fire(download_ds)
