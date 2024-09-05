import os
import json
import glob
import math
import fire

def process_audio_files(root_path, config_file, num_workers):
    # Read the config file
    with open(config_file, 'r') as f:
        config = json.load(f)
    print(len(config))
    # Find all .mp3 and .fail files
    mp3_files = set(glob.glob(f"{root_path}/**/*.mp3", recursive=True))
    fail_files = set(glob.glob(f"{root_path}/**/*.fail", recursive=True))

    # Extract IDs from file names
    mp3_ids = {os.path.splitext(os.path.basename(f))[0] for f in mp3_files}
    fail_ids = {os.path.splitext(os.path.basename(f))[0] for f in fail_files}
    # print(len(mp3_ids), len(fail_ids))
    # print(fail_ids)
    # Filter config based on found IDs
    filtered_config = [item for item in config if item['id'] not in mp3_ids and item['id'] not in fail_ids]

    # Create conf directory if it doesn't exist
    os.makedirs('conf', exist_ok=True)

    # Split the filtered config into num_workers parts
    chunk_size = math.ceil(len(filtered_config) / num_workers)
    for i in range(num_workers):
        start = i * chunk_size
        end = (i + 1) * chunk_size
        chunk = filtered_config[start:end]
        
        # Save each chunk to a separate file
        with open(f'conf/config_{i+1}.json', 'w') as f:
            json.dump(chunk, f, indent=2)

    print(f"Processed {len(filtered_config)} items into {num_workers} config files.")


if __name__ == '__main__':
    fire.Fire(process_audio_files)
# # Usage
# root_path = '/path/to/your/audio/files'
# config_file = 'youtube/local/all_data.json'
# num_workers = 4  # Adjust this as needed

# process_audio_files(root_path, config_file, num_workers)
