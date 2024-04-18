import json
import fire
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm

def fetch_video_metadata(channel_url):
    # Command to fetch video metadata
    command = [
        'yt-dlp',
        '--dump-json',  # Get metadata in JSON format
        '--flat-playlist',
        channel_url
    ]

    # Execute the command and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    video_data = result.stdout.strip().split('\n')
    return video_data

def download_audio(args):
    metadata, output_path = args
    metadata = json.loads(metadata)
    # print(metadata)
    print(metadata)
    video_id = metadata['id']
    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    download_command = [
        'yt-dlp',
        '-x',  # Extract audio
        '--audio-format', 'mp3',  # Set audio format to mp3
        '--audio-quality', '0',  # Set the best audio quality
        '-o', output_filename,  # Output filename template using video ID
        metadata['url']
    ]
    status = subprocess.run(download_command)
    if status.returncode != 0:
        print('download fail')
        return f"Failed to download {output_filename}", metadata
    return f"Downloaded {output_filename}", metadata

def worker_process(channel_url, output_path, num_workers=4, local_rank=0, num_ranks=1):
    video_metadata = fetch_video_metadata(channel_url)
    # Determine the segment of data this rank will handle
    # Create output path if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    segment_size = len(video_metadata) // num_ranks
    start_index = local_rank * segment_size
    end_index = (local_rank + 1) * segment_size if (local_rank + 1) < num_ranks else len(video_metadata)

    # Subset of metadata for this rank
    subset_metadata = video_metadata[start_index:end_index]

    # Save the subset metadata to a JSON file for this rank
    subset_metadata_path = os.path.join(output_path, f'metadata_rank_{local_rank}.json')
    with open(subset_metadata_path, 'w') as f:
        json.dump(subset_metadata, f, indent=4)

    params = [(metadata, output_path) for metadata in subset_metadata]
    # Use multiprocessing to download videos in parallel
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(pool.imap_unordered(download_audio, params),
                            total=len(subset_metadata), desc=f"Downloading videos for rank {local_rank}"))
    processed_metadata = [res[1] for res in results]

    subset_metadata_path = os.path.join(output_path, f'processed_metadata_rank_{local_rank}.json')
    with open(subset_metadata_path, 'w') as f:
        json.dump(processed_metadata, f, indent=4)
    return results

if __name__ == "__main__":
    # Example parameters
    fire.Fire(worker_process)
