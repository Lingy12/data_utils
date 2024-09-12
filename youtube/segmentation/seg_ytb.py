import os
import sys
import subprocess
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from queue import Queue
from threading import Thread
import fire

def get_duration(file_path):
    """Get the duration of an audio file using ffprobe."""
    cmd = [
        'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1', str(file_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error getting duration for {file_path}: {result.stderr}")
        return None
    return float(result.stdout)

def segment_mp3(input_file, channel, output_base):
    """Segment an MP3 file if it's longer than 30 seconds."""
    input_path = Path(input_file)
    output_dir = Path(output_base) / channel
    output_dir.mkdir(parents=True, exist_ok=True)
    
    duration = get_duration(input_path)
    if duration is None:
        return False
    
    if duration < 30:
        output_file = output_dir / input_path.name
        try:
            output_file.write_bytes(input_path.read_bytes())
        except IOError as e:
            print(f"Error copying {input_path}: {e}")
            return False
    else:
        output_pattern = output_dir / f"{input_path.stem}_%03d.mp3"
        cmd = [
            'ffmpeg', '-i', str(input_path), '-f', 'segment',
            '-segment_time', '30', '-c', 'copy', str(output_pattern)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error segmenting {input_path}: {result.stderr}")
            return False
    return True

def process_file(file, output_dir):
    try:
        channel = next(part for part in file.parts if part.startswith('@'))
    except StopIteration:
        print(f"Error: Could not extract channel name from {file}")
        return False
    
    return segment_mp3(file, channel, output_dir)

def producer(queue, source_dir):
    source_path = Path(source_dir)
    for file in source_path.rglob('*.mp3'):
        queue.put(file)
    queue.put(None)  # Signal the end of the queue

def consumer(queue, output_dir, pbar):
    while True:
        file = queue.get()
        if file is None:
            break
        success = process_file(file, output_dir)
        pbar.update(1)
        queue.task_done()

def segment_ytb(source_dir: str, output_dir: str, max_workers: int = os.cpu_count() * 2):
    """
    Segment MP3 files into 30-second chunks.

    Args:
        source_dir (str): Source directory containing MP3 files
        output_dir (str): Output directory for segmented files
        max_workers (int, optional): Maximum number of worker threads. Defaults to 2 * number of CPU cores.
    """
    source_path = Path(source_dir)
    total_files = sum(1 for _ in source_path.rglob('*.mp3'))
    
    queue = Queue(maxsize=max_workers * 2)  # Buffer for files to process
    
    # Start the producer thread
    producer_thread = Thread(target=producer, args=(queue, source_dir))
    producer_thread.start()
    
    # Create and start consumer threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=total_files, desc="Processing files") as pbar:
            futures = [executor.submit(consumer, queue, output_dir, pbar) for _ in range(max_workers)]
            concurrent.futures.wait(futures)
    
    producer_thread.join()
    
    print(f"\nProcessing complete. {total_files} files processed.")

if __name__ == "__main__":
    fire.Fire(segment_ytb)