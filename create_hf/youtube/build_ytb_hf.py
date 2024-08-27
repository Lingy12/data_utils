import os
import glob
from datasets import Dataset, Audio
from multiprocessing import Pool
from pathlib import Path
import tqdm
import fire

def process_channel(args):
    root_folder, channel = args
    target_files = glob.glob(os.path.join(root_folder, channel, '**', '*.mp3'), recursive=True)
    
    hf_dict = {"audio": [], 'channel': [], 'video_id': []}
    for f in target_files:
        audio = f
        f_base = os.path.basename(audio)
        video_id = Path(f_base).stem  # Correctly getting the stem instead of empty suffix
        
        hf_dict["audio"].append(audio)
        hf_dict['channel'].append(channel)
        hf_dict['video_id'].append(video_id)
    
    return hf_dict

def build_ytb_hf(root_folder, output_path, saving_workers):
    channels = os.listdir(root_folder)
    params = [(root_folder, channel) for channel in channels]
    
    with Pool(processes=saving_workers) as p:
        results = list(tqdm.tqdm(p.imap(process_channel, params), total=len(params)))
    
    combined_dict = {"audio": [], 'channel': [], 'video_id': []}
    for hf_d in results:
        for key in hf_d:
            combined_dict[key].extend(hf_d[key])  # Extending lists instead of overwriting
    # print(len(combined_dict['audio']), len(combined_dict['channel']), len(combined_dict['video_id']))
    ds = Dataset.from_dict(combined_dict)
    ds = ds.cast_column('audio', Audio(sampling_rate=16000))
    ds.save_to_disk(output_path, num_proc=saving_workers)
    
if __name__ == '__main__':
    fire.Fire(build_ytb_hf)
