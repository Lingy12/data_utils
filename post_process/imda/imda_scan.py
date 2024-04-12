from datasets import load_from_disk
import multiprocessing as mp
from tqdm import tqdm
import re
import fire

def check_entry(args):
    i, ds = args
    entry = ds[i]   
    
    pattern = r'(<([^>]+)>.*?<\/\2>)|(\(.*?\))|(\[.*?\])|(<.*?>)'
    matches = re.findall(pattern, entry['sentence'])
    # print(matches)
    contents = [item for match in matches for item in match if item]
    # print(entry['sentence'])
    # print(contents)
    # if len(contents) > 0:
    #     print(f'{i}'.center(50, '='))
    #     print(contents)
    
    return i, contents
    

def scan_ds(split, workers = 8):
    ds = load_from_disk(split).select_columns(['sentence'])

    params = [(i, ds) for i in range(len(ds))]
    
    with mp.Pool(workers) as p:
        res = list(tqdm(p.imap_unordered(check_entry, params), total=len(params)))
    
    special_set = set()
    for r in res:
        idx, match = r
        for m in match:
            special_set.add(m)
    print('===================================================')
    print(special_set)

if __name__ == '__main__':
    fire.Fire(scan_ds)