import json
import subprocess
import time
import os
from tqdm import tqdm
import fire
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
from multiprocessing import Pool
from local.download_funcs import download_audio_yt_dlp, download_audio_rapid
    

def download_single_audio(args):
    root_path, entry = args
    channel_path = os.path.join(root_path, entry['channel'])
    result = download_audio_rapid(entry, channel_path)
    print(result)
    
    if result['status'] == 'failed':
        return 'fail'

    return f"Downloaded {entry['channel']}: {result['status']}"

def download_data(data_config_path, root_path, total_device=1, device_index=0, max_workers=4):
    with open(data_config_path, 'r') as f:
        data_conf = json.load(f)
    
    print(f'Total device = {total_device}, device index = {device_index}')
    entries_per_device = math.ceil(len(data_conf) / total_device)
    start_index = device_index * entries_per_device
    end_index = min((device_index + 1) * entries_per_device, len(data_conf))
    print(f'Start index: {start_index}, End index: {end_index}')
    
    data_conf = data_conf[start_index:end_index]
    print(f'Total samples to download = {len(data_conf)}')

    with Pool(processes=max_workers) as pool:
        results = list(tqdm(
            pool.imap(download_single_audio, [(root_path, entry) for entry in reversed(data_conf)]),
            total=len(data_conf)
        ))

    fail_count = results.count('failed')
    total_count = len(results)
    print(f'Total downloads: {total_count}, Failed downloads: {fail_count}')
        
        
if __name__ == '__main__':
    fire.Fire(download_data)