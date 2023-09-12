from datasets import load_from_disk
import fire
from tqdm import tqdm
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

def check_entry(args):
    idx, ds = args # unpack
    try:
        _ = ds[idx]
    except:
        return -1, f'Check index {idx}'
    return 0, 'Success'


def check_data(hf_folder:str, num_worker: int = 4):
    ds = load_from_disk(hf_folder)
    N = len(ds)

    inputs = [(idx, ds) for idx in range(len(ds))]
    with Pool(processes=num_worker) as pool:
        results = list(tqdm(pool.imap(check_entry, inputs), total=N))
        
        for res in results:
            code, message = res
            
            if code == -1:
                print(message)
            # print(code, message)
        
    # for idx in tqdm(range(len(ds))):
        # try:
            # _ = ds[idx]
        # except:
            # print('Check index = {}'.format(idx))
    print('Dataset checked.')

if __name__ == "__main__":
    fire.Fire(check_data)



