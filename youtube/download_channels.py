import json
import fire
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm
import time
class VideoDownloader:
    def __init__(self):
        return 

    def fetch_video_metadata(self, channel_url, output_path):
        command = [
            'yt-dlp',
            '--dump-json',
            '--flat-playlist',
            '-4',
            channel_url,
            '--cookies', './cookies.txt'
        ]
        
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
        if result.returncode != 0:
            print('fail to fetch')
            return 
        video_data = result.stdout.strip().split('\n')
        
        metadata_path = os.path.join(output_path, 'metadata.json')
        if len(video_data) > 5:
            with open(metadata_path, 'w') as f:
                    json.dump(video_data, f, indent=4)

        return metadata_path


    def download_audio(self, args):
        metadata, output_path = args
        metadata = json.loads(metadata)
        video_id = metadata['id']
        output_filename = os.path.join(output_path, f"{video_id}.mp3")

        if os.path.exists(output_filename):
            metadata['status'] = 'already exists'
            return {"status": "success", "file": output_filename, "metadata": metadata}

        download_command = [
            'yt-dlp',
            '-x',
            '--audio-format', 'mp3',
            '--audio-quality', '5',
            '--postprocessor-args', "ffmpeg:-ar 16000",
            '-o', output_filename,
            '-4', 
            metadata['url'],
             '--cookies', './cookies.txt'
        ]

        max_retries = 10
        for attempt in range(max_retries):
            status = subprocess.run(download_command)
            if status.returncode == 0:
                return {"status": "success", "file": output_filename, "metadata": metadata}
            else:
                if attempt < max_retries - 1:  # Avoid sleep after the last attempt
                    print(f"Attempt {attempt + 1} failed, retrying...")
                    time.sleep(1)  # Wait for 5 seconds before retrying

        return {"status": "failed", "file": output_filename, "metadata": metadata}

    def worker_process(self, output_path, workers=4, local_rank=0, num_ranks=1, max_files_in_folder=100):
        meta_data_file = os.path.join(output_path, 'metadata.json')
        if not os.path.exists(meta_data_file):
            print('please build metadata first.')
            return
        with open(meta_data_file, 'r') as f:
            video_metadata = json.load(f)

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        segment_size = len(video_metadata) // num_ranks
        start_index = local_rank * segment_size
        end_index = (local_rank + 1) * segment_size if (local_rank + 1) < num_ranks else len(video_metadata)
        subset_metadata = video_metadata[start_index:end_index]

        rank_folder_path = os.path.join(output_path, f'{local_rank}')
        os.makedirs(rank_folder_path, exist_ok=True)

        num_subfolders = (len(subset_metadata) + max_files_in_folder - 1) // max_files_in_folder
        subfolder_names = [(chr(65 + i//26) + chr(65 + i%26)) for i in range(num_subfolders)]
        for name in subfolder_names:
            os.makedirs(os.path.join(rank_folder_path, name), exist_ok=True)

        folder_assignments = [os.path.join(rank_folder_path, subfolder_names[i % num_subfolders]) for i in range(len(subset_metadata))]
        params = list(zip(subset_metadata, folder_assignments))

        success_count = 0
        fail_count = 0
        with Pool(processes=workers) as pool:
            for result in tqdm(pool.imap_unordered(self.download_audio, params), total=len(subset_metadata), desc=f"Downloading videos for rank {local_rank}"):
                if result['status'] == "success":
                    success_count += 1
                else:
                    fail_count += 1

        counts = {"success_count": success_count, "fail_count": fail_count}
        with open(os.path.join(rank_folder_path, f'count_{local_rank}.json'), 'w') as f:
            json.dump(counts, f, indent=4)

if __name__ == "__main__":
    fire.Fire(VideoDownloader)
