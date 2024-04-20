import json
import fire
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm

def fetch_video_metadata(channel_url):
    command = [
        'yt-dlp',
        '--dump-json',
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
    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    download_command = [
        'yt-dlp',
        '-x',
        '--audio-format', 'mp3',
        '--audio-quality', '0',
        '--postprocessor-args', "ffmpeg:-ar 16000",
        '-o', output_filename,
        metadata['url'],
        '--cookies', "./cookies.txt"
    ]
    status = subprocess.run(download_command)
    if status.returncode == 0:
        return {"status": "success", "file": output_filename, "metadata": metadata}
    else:
        return {"status": "failed", "file": output_filename, "metadata": metadata}

def worker_process(channel_url, output_path, workers=4, local_rank=0, num_ranks=1, max_files_in_folder=100):
    video_metadata = fetch_video_metadata(channel_url)
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
        for result in tqdm(pool.imap_unordered(download_audio, params), total=len(subset_metadata), desc=f"Downloading videos for rank {local_rank}"):
            if result['status'] == "success":
                success_count += 1
            else:
                fail_count += 1

    counts = {"success_count": success_count, "fail_count": fail_count}
    with open(os.path.join(rank_folder_path, f'count_{local_rank}.json'), 'w') as f:
        json.dump(counts, f, indent=4)

if __name__ == "__main__":
    fire.Fire(worker_process)
