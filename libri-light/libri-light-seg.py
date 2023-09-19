from datasets import Dataset, IterableDataset, Audio, load_from_disk
from glob import glob
import uuid
import os
import fire
import librosa
from tqdm import tqdm
import json
from multiprocessing import Pool
from typing import Tuple

schema = ["id", "audio", "sentence"]
meta_key = ["speaker_id", "book_id"] # removed VOD
schema += meta_key

# Change this part for extension
def process_file(args) -> Tuple[int, str, str, str, dict]:
    file, audio_check, sr, sid, bid = args
    file_path = file
    
    meta_data = {}
    meta_data['speaker_id'] = sid
    meta_data['book_id'] = bid

    if audio_check:
        try:
            _, _ = librosa.load(file_path, sr=sr)# load to single channel
        except :
            return -1, f"{file_path} audio failed during checking", "", "", {}
    return 1, str(uuid.uuid4()), str(file_path), str(""), meta_data

def form_ds(librilght_raw:str, dest:str, sr=None, audio_check:bool=True, num_worker:int = 4):
    print('Scanning audio files...')
    files = glob(os.path.join(librilght_raw,'**/**/*.flac')) # Can define a different file structure
    print('Audio files scanned.')
    if audio_check:
        print('Running with audio check. It might be slow.')
    ds_dict = {key: [] for key in schema}
    
    print('Total audio files = {}'.format(len(files)))
    inputs = []
    inputs = [(file, audio_check, sr, os.path.dirname(os.path.dirname(file)), os.path.dirname(file)) for file in files]
 

    with Pool(processes=num_worker) as pool:
        results = list(tqdm(pool.imap(process_file, inputs), total=len(inputs)))
        
        for res in results:
            status = res[0]
            if status == 1:
                status, id, file_path, sentence, meta_data = res
                ds_dict["id"].append(id)
                # ds_dict["path"].append(str(file_path))
                ds_dict["audio"].append(str(file_path))
                ds_dict["sentence"].append(sentence) # no transcript
    # 
                for meta in meta_key:
                    ds_dict[meta].append(meta_data[meta])

    ds = Dataset.from_dict(ds_dict)

    if sr == None:
        ds = ds.cast_column('audio', Audio())
    else:
        ds = ds.cast_column("audio", Audio(sampling_rate=sr))
    ds.save_to_disk(dest, num_proc=num_worker)

def load_ds(ds_path):
    return load_from_disk(ds_path)

if __name__ == '__main__':
    fire.Fire(form_ds)
