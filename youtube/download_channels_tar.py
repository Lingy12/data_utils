import json
import fire
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm
import tarfile
import io

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
            print('Fail to fetch')
            return
        video_data = result.stdout.strip().split('\n')

        metadata_path = os.path.join(output_path, 'metadata.json')
        if len(video_data) > 5:
            with open(metadata_path, 'w') as f:
                json.dump(video_data, f, indent=4)
        return metadata_path

    def download_audio(self, metadata, output_subfolder):
        metadata = json.loads(metadata)
        video_id = metadata['id']
        output_filename = f"{video_id}.mp3"
        full_path = os.path.join(output_subfolder, output_filename)

        if os.path.exists(full_path):
            return output_subfolder, output_filename, None, metadata  # Skip download if file already exists

        download_command = [
            'yt-dlp',
            '-x',
            '--audio-format', 'mp3',
            '--audio-quality', '5',
            '--postprocessor-args', "ffmpeg:-ar 16000",
            '-o', '-',  # Output to stdout
            '-4',
            metadata['url'],
            '--cookies', './cookies.txt'
        ]

        max_retries = 3  # Number of retries for downloading
        for attempt in range(max_retries):
            process = subprocess.Popen(download_command, stdout=subprocess.PIPE)
            audio_data, _ = process.communicate()

            if process.returncode == 0:
                return output_subfolder, output_filename, audio_data, metadata
            else:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed, retrying...")
                    time.sleep(5)  # Wait for 5 seconds before retrying

        return output_subfolder, output_filename, None, metadata  # Return None if all retries fail

    def worker_process(self, output_path, channel_url, workers=4, local_rank=0, num_ranks=1, max_files_in_folder=100):
        metadata_path = self.fetch_video_metadata(channel_url, output_path)
        if not metadata_path:
            print('Metadata not available')
            return

        with open(metadata_path, 'r') as f:
            video_metadata = json.load(f)

        segment_size = len(video_metadata) // num_ranks
        start_index = local_rank * segment_size
        end_index = (local_rank + 1) * segment_size if (local_rank + 1) < num_ranks else len(video_metadata)
        subset_metadata = video_metadata[start_index:end_index]

        num_subfolders = (len(subset_metadata) + max_files_in_folder - 1) // max_files_in_folder
        subfolder_names = [(chr(65 + i//26) + chr(65 + i%26)) for i in range(num_subfolders)]
        folder_assignments = [os.path.join(str(local_rank), subfolder_names[i % num_subfolders]) for i in range(len(subset_metadata))]
        tasks = [(metadata, folder) for metadata, folder in zip(subset_metadata, folder_assignments)]

        tar_filename = os.path.join(output_path, f'{local_rank}.tar')
        with tarfile.open(tar_filename, "w") as tar:
            with Pool(processes=workers) as pool:
                for result in tqdm(pool.starmap(self.download_audio, tasks), total=len(subset_metadata), desc=f"Downloading videos for rank {local_rank}"):
                    output_subfolder, filename, audio_data, metadata = result
                    if audio_data:  # Only add to tar if download was successful
                        full_path = os.path.join(output_subfolder, filename)
                        tarinfo = tarfile.TarInfo(name=full_path)
                        tarinfo.size = len(audio_data)
                        tar.addfile(tarinfo, io.BytesIO(audio_data))

                        # Add individual metadata files into the tar as well
                        metadata_content = json.dumps(metadata, indent=4).encode('utf-8')
                        meta_tarinfo = tarfile.TarInfo(name=f"{full_path}_metadata.json")
                        meta_tarinfo.size = len(metadata_content)
                        tar.addfile(meta_tarinfo, io.BytesIO(metadata_content))

if __name__ == "__main__":
    fire.Fire(VideoDownloader)
