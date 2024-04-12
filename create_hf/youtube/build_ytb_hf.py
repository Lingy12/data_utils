import fire
from glob import glob
import os
from datasets import Dataset, Audio

def build_ytb_hf(root_folder, output_path, saving_workers):
    target_files = glob(os.path.join(root_folder, '**', '*.wav'))
    print(len(target_files))
    
    hf_dict = {"audio": [], 'seg_id': [], 'video_id': []}
    for f in target_files:
        audio = f
        f_base = os.path.basename(audio)
        video_id, seg_id = f_base.split('_') 
        
        for k in hf_dict:
            hf_dict[k].append(eval(k))
    
    ds = Dataset.from_dict(hf_dict)
    ds = ds.cast_column('audio', Audio())
    ds.save_to_disk(output_path)
    
if __name__ == '__main__':
    fire.Fire(build_ytb_hf)