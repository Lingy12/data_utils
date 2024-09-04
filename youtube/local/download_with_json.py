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
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

def download_single_audio(args):
    root_path, entry = args
    channel_path = os.path.join(root_path, entry['channel'])
    result = download_audio_rapid(entry, channel_path)
    logger.info(result)
    
    if result['status'] == 'failed':
        return 'fail'

    return f"Downloaded {entry['channel']}: {result['status']}"

def download_data(data_config_path, root_path, total_device=1, device_index=0, max_workers=4, skip_index=0):
    with open(data_config_path, 'r') as f:
        data_conf = json.load(f)
    
    logger.info(f'Total device = {total_device}, device index = {device_index}')
    entries_per_device = math.ceil(len(data_conf) / total_device)
    start_index = device_index * entries_per_device + skip_index
    end_index = min((device_index + 1) * entries_per_device, len(data_conf))
    logger.info(f'Start index: {start_index}, End index: {end_index}')
    
    data_conf = data_conf[start_index:end_index]
    logger.info(f'Total samples to download = {len(data_conf)}')

    with Pool(processes=max_workers) as pool:
        fail_count = 0
        total_count = 0
        
        for result in tqdm(pool.imap_unordered(download_single_audio, [(root_path, entry) for entry in reversed(data_conf)]), total=len(data_conf)):
            logger.info(result)  # Print the result as it comes in
            total_count += 1
            if result == 'fail':
                fail_count += 1

    logger.info(f'Total downloads: {total_count}, Failed downloads: {fail_count}')
        
        
if __name__ == '__main__':
    fire.Fire(download_data)