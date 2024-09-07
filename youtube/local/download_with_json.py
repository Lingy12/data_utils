import json
import subprocess
import time
import os
from tqdm import tqdm
import fire
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
from multiprocessing import Pool, Manager
from local.download_funcs import download_audio_yt_dlp, download_audio_rapid
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

def download_single_audio(args):
    root_path, entry = args
    channel_path = os.path.join(root_path, entry['channel'])
    
    # Try yt-dlp download first
    result = download_audio_yt_dlp(entry, channel_path)
    logger.info(f"yt-dlp download result: {result}")
    
    if result['status'] == 'failed':
        # If yt-dlp download fails, try rapid
        logger.info("yt-dlp download failed. Trying rapid download...")
        result = download_audio_rapid(entry, channel_path)
        logger.info(f"Rapid download result: {result}")
    
    if result['status'] == 'failed':
        return 'fail'

    return f"Downloaded {entry['channel']}: {result['status']}"

def download_data(data_config_path, root_path, total_device=1, device_index=0, max_workers=50, skip_index=0):
    with open(data_config_path, 'r') as f:
        data_conf = json.load(f)
    
    logger.info(f'Total device = {total_device}, device index = {device_index}')
    entries_per_device = math.ceil(len(data_conf) / total_device)
    start_index = device_index * entries_per_device + skip_index
    end_index = min((device_index + 1) * entries_per_device, len(data_conf))
    logger.info(f'Start index: {start_index}, End index: {end_index}')
    
    data_conf = data_conf[start_index:end_index]
    logger.info(f'Total samples to download = {len(data_conf)}')

    with Manager() as manager:
        consecutive_fails = manager.Value('i', 0)
        max_consecutive_fails = 50

        def process_result(result):
            nonlocal consecutive_fails
            if result == 'fail':
                consecutive_fails.value += 1
                if consecutive_fails.value >= max_consecutive_fails:
                    logger.error(f"Stopping process due to {max_consecutive_fails} consecutive failures")
                    pool.terminate()
                return 1
            else:
                consecutive_fails.value = 0
                return 0

        with Pool(processes=max_workers) as pool:
            fail_count = 0
            total_count = 0
            
            try:
                for result in tqdm(pool.imap_unordered(download_single_audio, [(root_path, entry) for entry in reversed(data_conf)]), total=len(data_conf)):
                    total_count += 1
                    fail_count += process_result(result)
                    if consecutive_fails.value >= max_consecutive_fails:
                        break
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                pool.terminate()

        logger.info(f'Total downloads: {total_count}, Failed downloads: {fail_count}')

if __name__ == '__main__':
    fire.Fire(download_data)