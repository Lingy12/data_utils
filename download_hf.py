from datasets import load_dataset
import fire
def download_ds(ds_hf_name:str, dest: str):
    print('Downloading and load dataset from hugging face hub...')
    ds = load_dataset(ds_hf_name)
    print('Dataset loaded')

    print('Saving dataset...')
    ds.save_to_disk(dest)
    print('Dataset saved.')

if __name__ == "__main__":
    fire.Fire(download_ds)
