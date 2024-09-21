import fire
from datasets import Dataset, Audio, Features, Value
import os
import tqdm

def build_audio_dataset(input_folder, output_path, num_proc=32):
    # Find all MP3 files recursively
    mp3_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith('.mp3'):
                mp3_files.append(os.path.join(root, file))

    # Prepare dataset
    data = {
        'path': mp3_files,
        'audio': mp3_files
    }
    print(len(data['path']))
    # Create dataset
    features = Features({
        'path': Value('string'),
        'audio': Audio(sampling_rate=16000)
    })
    
    dataset = Dataset.from_dict(data, features=features)

    # Save the dataset
    dataset.save_to_disk(output_path, num_proc=num_proc)
    print(f"Dataset saved to {output_path}")

if __name__ == "__main__":
    fire.Fire(build_audio_dataset)
