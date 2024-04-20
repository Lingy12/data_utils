from datasets import load_dataset
import fire

def download_ds(dest1: str, dest2: str, num_worker:int = 4):
    print('Downloading and load dataset from hugging face hub...')
    ds_1 = load_dataset("edinburghcstr/ami", "ihm")
    ds_2 = load_dataset("edinburghcstr/ami", "sdm")

   # ds = load_dataset(ds_hf_name, num_proc=num_worker, cache_dir=dest)
    print('Dataset loaded')

    print('Saving dataset...')
    ds_1.save_to_disk(dest1, num_proc=num_worker)
    ds_2.save_to_disk(dest2, num_proc=num_worker)
    print('Dataset saved.')

    ds_1.cleanup_cache_files()
    ds_2.cleanup_cache_files()
    print('Local cache cleared')

if __name__ == "__main__":
    fire.Fire(download_ds)
