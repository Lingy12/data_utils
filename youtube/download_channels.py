import json
import fire
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm
import random
import string

def fetch_video_metadata(channel_url):
    command = [
        'yt-dlp',
        '--dump-json',  # Get metadata in JSON format
        '--flat-playlist',
        channel_url,
        '--cookies', "./cookies.txt"
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    video_data = result.stdout.strip().split('\n')
    return video_data

def download_audio(args):
    metadata, output_path = args
    metadata = json.loads(metadata)
    video_id = metadata['id']

    # Randomly choose a subfolder within the output_path
    # sub_folders = os.listdir(output_path)

    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    download_command = [
        'yt-dlp',
        '-x',  # Extract audio
        '--audio-format', 'mp3',  # Set audio format to mp3
        '--audio-quality', '0',  # Set the best audio quality
        '-o', output_filename,  # Output filename template using video ID
        metadata['url'],
        '--cookies', "./cookies.txt"
    ]
    status = subprocess.run(download_command)
    if status.returncode != 0:
        print('Download failed')
        return f"Failed to download {output_filename}", metadata
    return f"Downloaded {output_filename}", metadata

def worker_process(channel_url, output_path, workers=4, local_rank=0, num_ranks=1, max_files_in_folder=100):
    video_metadata = fetch_video_metadata(channel_url)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    segment_size = len(video_metadata) // num_ranks
    start_index = local_rank * segment_size
    end_index = (local_rank + 1) * segment_size if (local_rank + 1) < num_ranks else len(video_metadata)
    subset_metadata = video_metadata[start_index:end_index]

    # Create a folder for each rank
    rank_folder_path = os.path.join(output_path, f'{local_rank}')
    if not os.path.exists(rank_folder_path):
        os.makedirs(rank_folder_path)

    subset_metadata_path = os.path.join(rank_folder_path, f'metadata_rank_{local_rank}.json')
    with open(subset_metadata_path, 'w') as f:
        json.dump(subset_metadata, f, indent=4)
    
    # Calculate the required number of subfolders
    num_files = len(subset_metadata)
    num_subfolders = (num_files + max_files_in_folder - 1) // max_files_in_folder
    # Generate subfolder names like 'AA', 'BB', ..., 'ZZ', and recycle if more are needed
    subfolder_names = [(chr(65 + i//26) + chr(65 + i%26)) for i in range(num_subfolders)]
    for name in subfolder_names:
        os.makedirs(os.path.join(rank_folder_path, name), exist_ok=True)

    # Assign each file to a subfolder
    folder_assignments = [os.path.join(rank_folder_path, subfolder_names[i % num_subfolders]) for i in range(num_files)]
    # print(folder_assignments)
    params = list(zip(subset_metadata, folder_assignments))

    with Pool(processes=workers) as pool:
        results = list(tqdm(pool.imap_unordered(download_audio, params),
                            total=len(subset_metadata), desc=f"Downloading videos for rank {local_rank}"))

    processed_metadata = [res[1] for res in results]
    subset_metadata_path = os.path.join(rank_folder_path, f'processed_metadata_rank_{local_rank}.json')
    with open(subset_metadata_path, 'w') as f:
        json.dump(processed_metadata, f, indent=4)
    return results

if __name__ == "__main__":
    fire.Fire(worker_process)
