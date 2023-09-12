from datasets import load_from_disk
import fire
from tqdm import tqdm

def check_data(hf_folder:str):
    ds = load_from_disk(hf_folder)

    for idx in tqdm(range(len(ds))):
        try:
            _ = ds[idx]
        except:
            print('Check index = {}'.format(idx))
    print('Dataset checked.')

if __name__ == "__main__":
    fire.Fire(check_data)



