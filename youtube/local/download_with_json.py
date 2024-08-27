import json
import subprocess
import time
import os
from tqdm import tqdm
import fire
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
def download_audio(metadata, output_path):
    # metadata, output_path = args
    # metadata = json.loads(metadata)
    video_id = metadata['id']
    
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    # print(output_filename)
    if os.path.exists(output_filename):
        # print('already exists')
        return {"status": "success", "file": output_filename, "metadata": metadata}

    max_retries = 3
    for attempt in range(max_retries):
        download_command = [
        'yt-dlp',
        '-x',
        '--audio-format', 'mp3',
        '--audio-quality', '5',
        '--postprocessor-args', "ffmpeg:-ar 16000",
        '-o', output_filename,
        '-4', 
        '-q',
        '--no-warnings',
        '--username', 'oauth2', '--password', '',
        metadata['url'],
    ]
        status = subprocess.run(download_command)
        if status.returncode == 0:
            assert os.path.exists(output_filename)
            return {"status": "success", "file": output_filename, "metadata": metadata}
        else:
            if attempt < max_retries - 1:  # Avoid sleep after the last attempt
                print(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(10)  # Wait for 5 seconds before retrying

    return {"status": "failed", "file": output_filename, "metadata": metadata}
    

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
    result = download_audio(entry, channel_path)
    
    if result['status'] == 'fail':
        if manager.increment_failures():
            return None  # Signal to stop
    else:
        manager.reset_failures()
    
    return f"Downloaded {entry['channel']}: {result['status']}"

def download_data(data_config_path, root_path, total_device = 1, device_index = 0, max_workers=4, failure_threshold=50):
    with open(data_config_path, 'r') as f:
        data_conf = json.load(f)
    
    entries_per_device = math.ceil(len(data_conf) / total_device)
    start_index = device_index * entries_per_device
    end_index = min((device_index + 1) * entries_per_device, len(data_conf))
    data_conf = data_conf[start_index:end_index]
    # data_conf = 
    manager = DownloadManager(failure_threshold)
    # results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_single_audio, entry, root_path, manager): entry for entry in data_conf}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading audio files"):
            result = future.result()
            if result is None:  # Check if we should stop
                print(f"Stopping due to {failure_threshold} consecutive failures.")
                executor.shutdown(wait=False, cancel_futures=True)
                break
            
            # results.append(result)
            #print(result)  # Print result as soon as it's available
            
            if manager.should_stop:
                print(f"Stopping due to {failure_threshold} consecutive failures.")
                executor.shutdown(wait=False, cancel_futures=True)
                break
        
        
if __name__ == '__main__':
    fire.Fire(download_data)