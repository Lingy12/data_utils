import os
import fire
from datasets import Dataset, Audio

def find_audio_files(root_dir):
    """Find all .wav files within the 'audio' directories under the given root directory."""
    audio_files = {}
    for subdir, dirs, files in os.walk(root_dir):
        if 'audio' in os.path.basename(subdir):
            for file in files:
                if file.endswith('.wav'):
                    file_path = os.path.join(subdir, file)
                    filename = os.path.basename(file_path).split('.')[0]
                    audio_files[filename] = file_path
    return audio_files

def load_transcripts(text_file):
    """Load transcripts from a text file."""
    transcript_map = {}
    with open(text_file, 'r') as file:
        for line in file:
            parts = line.strip().split(' ', 1)
            if len(parts) > 1:
                transcript_map[parts[0]] = parts[1]
    return transcript_map

def create_dataset(transcript_map, audio_files):
    """Create a dictionary with lists for files and transcripts."""
    data = {'audio': [], 'sentence': [], 'file_name': []}
    for filename, transcript in transcript_map.items():
        audio_file = audio_files.get(filename)
        if audio_file:
            data['audio'].append(audio_file)
            data['file_name'].append(audio_file)
            data['sentence'].append(transcript)
        else:
            print(f"Warning: Audio file for {filename} not found.")
    return Dataset.from_dict(data)

def build_data(root_dir, output_path, num_workers=16):
    os.makedirs(output_path, exist_ok=True)
    text_file = os.path.join(root_dir, 'text')

    # Process files
    audio_files = find_audio_files(root_dir)
    transcript_map = load_transcripts(text_file)
    dataset = create_dataset(transcript_map, audio_files)
    dataset = dataset.cast_column('audio', Audio(sampling_rate=16000))

    print(dataset)
    dataset.save_to_disk(output_path, num_proc=num_workers)

if __name__ == "__main__":
    fire.Fire(build_data)

