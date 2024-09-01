import json
import subprocess
import time
import os
from tqdm import tqdm
import fire
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
from local.download_funcs import download_audio_yt_dlp, download_audio_rapid
    

class DownloadManager:
    def __init__(self, failure_threshold=50):
        self.consecutive_failures = 0
        self.failure_threshold = failure_threshold
        self.lock = threading.Lock()
        self.should_stop = False

    def reset_failures(self):
        with self.lock:
            self.consecutive_failures = 0

    def increment_failures(self):
        with self.lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.failure_threshold:
                self.should_stop = True
        return self.should_stop

def download_single_audio(entry, root_path, manager):
    channel_path = os.path.join(root_path, entry['channel'])
    result = download_audio_yt_dlp(entry, channel_path)
    
    if result['status'] == 'fail':
        if manager.increment_failures():
            return None  # Signal to stop
        else:
            return 'fail'
    else:
        manager.reset_failures()
    
    return f"Downloaded {entry['channel']}: {result['status']}"

def download_data(data_config_path, root_path, total_device = 1, device_index = 0, max_workers=4, failure_threshold=50):
    with open(data_config_path, 'r') as f:
        data_conf = json.load(f)
    print('total device = {}, deivce index = {}'.format(total_device, device_index))
    entries_per_device = math.ceil(len(data_conf) / total_device)
    start_index = device_index * entries_per_device
    end_index = min((device_index + 1) * entries_per_device, len(data_conf))
    print(start_index, end_index)
    data_conf = data_conf[start_index:end_index]
    print('total samples to download = {}'.format(len(data_conf)))
    # data_conf = 
    manager = DownloadManager(failure_threshold)
    # results = []
    fail_count = 0
    total_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_single_audio, entry, root_path, manager): entry for entry in data_conf}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading audio files"):
            result = future.result()
            if result is None:  # Check if we should stop
                print(f"Stopping due to {failure_threshold} consecutive failures.")
                executor.shutdown(wait=False, cancel_futures=True)
                break
            total_count += 1
            if result == 'fail':
                fail_count += 1
                print(f'Current fail count = {fail_count}, current total count = {total_count}')
            # results.append(result)
            #print(result)  # Print result as soon as it's available
            
            if manager.should_stop:
                print(f"Stopping due to {failure_threshold} consecutive failures.")
                executor.shutdown(wait=False, cancel_futures=True)
                break
        
        
if __name__ == '__main__':
    fire.Fire(download_data)