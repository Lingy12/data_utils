import json
import fire
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm
import tarfile
import io

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

def download_audio(metadata):
    metadata = json.loads(metadata)
    video_id = metadata['id']
    output_filename = f"{video_id}.mp3"

    download_command = [
        'yt-dlp',
        '-x',
        '--audio-format', 'mp3',
        '--postprocessor-args', "ffmpeg:-ar 16000",  # Set audio sampling rate to 16000Hz
        '--audio-quality', '0',
        '--output', '-',  # Output to stdout
        metadata['url'],
        '--cookies', "./cookies.txt"
    ]
    process = subprocess.Popen(download_command, stdout=subprocess.PIPE)
    audio_data, _ = process.communicate()

    if process.returncode == 0:
        return output_filename, audio_data, metadata
    else:
        return output_filename, None, metadata

def worker_process(channel_url, output_path, workers=4, local_rank=0, num_ranks=1, max_files_in_folder=100):
    video_metadata = fetch_video_metadata(channel_url)
    segment_size = len(video_metadata) // num_ranks
    start_index = local_rank * segment_size
    end_index = (local_rank + 1) * segment_size if (local_rank + 1) < num_ranks else len(video_metadata)
    subset_metadata = video_metadata[start_index:end_index]

    # Ensure the output path exists
    os.makedirs(output_path, exist_ok=True)

    # Create subfolder paths for each video
    num_subfolders = (len(subset_metadata) + max_files_in_folder - 1) // max_files_in_folder
    subfolder_names = [(chr(65 + i//26) + chr(65 + i%26)) for i in range(num_subfolders)]
    folder_assignments = [subfolder_names[i % num_subfolders] for i in range(len(subset_metadata))]

    tar_filename = os.path.join(output_path, f'{local_rank}.tar')
    results = []
    with Pool(processes=workers) as pool:
        results = list(tqdm(pool.imap_unordered(download_audio, subset_metadata),
                            total=len(subset_metadata), desc=f"Downloading videos for rank {local_rank}"))

    with tarfile.open(tar_filename, "w") as tar:
        # Add original metadata to the tar
        original_meta = io.BytesIO(json.dumps(subset_metadata).encode('utf-8'))
        tarinfo = tarfile.TarInfo(name=f"{local_rank}/metadata_rank_{local_rank}.json")
        tarinfo.size = len(original_meta.getvalue())
        tar.addfile(tarinfo, original_meta)

        # Process results and metadata
        processed_metadata = []
        for (filename, audio_data, metadata), folder in zip(results, folder_assignments):
            if audio_data:
                full_path = os.path.join(f"{local_rank}", folder, filename)
                tarinfo = tarfile.TarInfo(name=full_path)
                tarinfo.size = len(audio_data)
                tar.addfile(tarinfo, io.BytesIO(audio_data))
                processed_metadata.append(metadata)

        # Add processed metadata to the tar
        processed_meta = io.BytesIO(json.dumps(processed_metadata, indent=4).encode('utf-8'))
        tarinfo = tarfile.TarInfo(name=f"{local_rank}/processed_metadata_rank_{local_rank}.json")
        tarinfo.size = len(processed_meta.getvalue())
        tar.addfile(tarinfo, processed_meta)

if __name__ == "__main__":
    fire.Fire(worker_process)
