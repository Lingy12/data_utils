import os
import subprocess
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
import fire

def get_mp3_files(directory):
    """Yield full file paths for all mp3 files in the given directory."""
    path = Path(directory)
    return list(path.rglob('*.mp3'))

def get_duration(file_path):
    """Use ffprobe to get the duration of an MP3 file in seconds."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', str(file_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return float(result.stdout.strip())
    except ValueError:
        print(f"Error processing file: {file_path}")
        return 0

def calculate_total_duration(directory, workers=4):
    """Calculate the total duration of all MP3 files in a directory using parallel processing."""
    files = get_mp3_files(directory)
    total_duration = 0
    with Pool(workers) as pool:
        for duration in tqdm(pool.imap_unordered(get_duration, files), total=len(files), desc="Processing MP3 Files"):
            total_duration += duration
    return total_duration

def main(directory, workers=4):
    """Calculate the total hours of MP3 files in a given directory."""
    if not os.path.exists(directory):
        print("The specified directory does not exist.")
        return

    total_duration_seconds = calculate_total_duration(directory, workers)
    total_hours = total_duration_seconds / 3600
    print(f"Total duration: {total_hours:.2f} hours")

if __name__ == '__main__':
    fire.Fire(main)

