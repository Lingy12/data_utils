from val_utils_mp import identify_language, init_whisper_model

from glob import glob
import os 
import random
from tqdm import tqdm
import multiprocessing as mp
import os
import fire

def check_folder(root, trail):
    mp3_files = glob(os.path.join(root, '**', '*.mp3'), recursive=True)
    # print(mp3_files)
    if len(mp3_files) < trail:
        return {"ms": 0, "en": 0}
    def update_score(existing, added):
        for k in added:
            if added[k] < 0.1:
                continue # only keep > 0.1
            if k not in existing:
                existing[k] = (added[k], 1)
            else:
                existing[k] = (existing[k][0] + added[k], existing[k][1] + 1)
        return existing
    agg_top_10 = {}
    # random.choices(mp3_files)
    for f in tqdm(random.choices(mp3_files, k=trail)):
        score = identify_language(f)
        
        agg_top_10 = update_score(agg_top_10, score)
    return {k: agg_top_10[k][0] / agg_top_10[k][1] for k in agg_top_10}

def verify_folder(args):
    root, folder = args
    init_whisper_model()
    score = check_folder(os.path.join(root, folder), 50)
    print(folder, score)      
    if 'ms' not in score:
        return 0, folder
    if score['ms'] > 0.5 and score['en'] > 0.1:
        return 1, folder
    else:
        return 0, folder
    
def verify_all(root, workers=4):
    sub_dirs = os.listdir(root)
    params = [(root, sub_dir) for sub_dir in sub_dirs]
    with mp.Pool(processes=workers) as p:
        results = tqdm(p.imap_unordered(verify_folder, params), total=len(params))
    # results = []
    # for p in tqdm(params):
    #     results.append(verify_folder(p))
    valid_channel = []
    for res in results:
        if res[0] == 1:
            valid_channel.append(res[1])
    print('Valid channels are: {}'.format(valid_channel))

if __name__ == '__main__':
    fire.Fire(verify_all)
            