import subprocess
import json
import os
import fire

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

def download_audio(video_metadata, id_to_title, output_path):
    # Parse metadata
    import json
    metadata = json.loads(video_metadata)
    video_id = metadata['id']
    video_title = metadata['title']
    
    # Add to dictionary
    id_to_title[video_id] = video_title
    
    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    # Command to download only audio and save as MP3
    download_command = [
        'yt-dlp',
        '-x',  # Extract audio
        '--audio-format', 'mp3',  # Set audio format to mp3
        '--audio-quality', '0',  # Set the best audio quality
        '-o', output_filename,  # Output filename template using video ID
        metadata['url']
    ]

    # Execute the download command
    subprocess.run(download_command)
    print(f"Downloaded {video_id}.mp3")

def download_videos(channel_url, output_path):
    # Fetch video metadata
    video_metadatas = fetch_video_metadata(channel_url)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Dictionary to store ID to title mapping
    id_to_title = {}

    # Download audio for each video
    for video_metadata in video_metadatas:
        download_audio(video_metadata, id_to_title, output_path)

    # Print or save the dictionary as needed
    print(id_to_title)
    # Optionally, save to a file
    json_path = os.path.join(output_path, 'id_to_title_mapping.json')
    with open(json_path, 'w') as f:
        json.dump(id_to_title, f, indent=4)


if __name__ == "__main__":
    fire.Fire(download_videos)
