import os
import sys
import subprocess
import argparse
from pathlib import Path
from tqdm import tqdm

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

def main(source_dir, output_dir):
    source_path = Path(source_dir)
    mp3_files = list(source_path.rglob('*.mp3'))
    
    for file in tqdm(mp3_files, desc="Processing files"):
        try:
            channel = next(part for part in file.parts if part.startswith('@'))
        except StopIteration:
            print(f"Error: Could not extract channel name from {file}")
            continue
        
        success = segment_mp3(file, channel, output_dir)
        if not success:
            print(f"Failed to process {file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Segment MP3 files into 30-second chunks.")
    parser.add_argument("source_dir", help="Source directory containing MP3 files")
    parser.add_argument("output_dir", help="Output directory for segmented files")
    args = parser.parse_args()

    main(args.source_dir, args.output_dir)