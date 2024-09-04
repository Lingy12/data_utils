import requests
from local.secret import rapid_key
import sys
from pydub import AudioSegment
import os
import subprocess
import time

# target_url = sys.argv[1]
def download_audio_rapid(metadata, output_path):
    video_id = metadata['id']
    
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    
    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    fail_filename = os.path.join(output_path, f"{video_id}.fail")
    
    # Check if .fail file exists
    if os.path.exists(fail_filename):
        return {"status": "failed", "file": output_filename, "metadata": metadata}
    
    if os.path.exists(output_filename):
        return {"status": "success", "file": output_filename, "metadata": metadata}
    
    url = "https://youtube86.p.rapidapi.com/api/youtube/links"
    headers = {
        'Content-Type': 'application/json',
        'x-rapidapi-host': 'youtube86.p.rapidapi.com',
        'x-rapidapi-key': rapid_key  # Replace <key> with your actual API key
    }
    data = {
        "url": metadata['url']
    }
    
    response = requests.post(url, headers=headers, json=data)
    # print('fetched audio links')
    if response.status_code == 200:
        result = response.json()
        # print(result[0]['urls'][0]['url'])
        # Extract the video ID from the URL
        video_id = metadata['id']
        
        # Iterate through each URL with audio = true and isBundled = true
        for url in result[0]['urls']:
            if url.get('isBundle'):
                # print('find bundle')
                audio_url = url['url']  # Get the audio URL
                extension = url['extension']  # Get the extension
                # print(url, extension)
                
                # Add retry logic for fetching audio_response
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        audio_response = requests.get(audio_url)  # Add timeout
                        audio_response.raise_for_status()  # Raise an exception for bad status codes
                        break  # If successful, break the retry loop
                    except (requests.RequestException, requests.Timeout) as e:
                        if attempt < max_retries - 1:
                            print(f"Attempt {attempt + 1} failed, retrying... Error: {str(e)}")
                            time.sleep(1)  # Wait for 1 second before retrying
                        else:
                            print(f"Failed to fetch audio after {max_retries} attempts.")
                            return {"status": "failed", "file": output_filename, "metadata": metadata}

                if audio_response.status_code == 200:
                    temp_filename = os.path.join(output_path, f"{video_id}.{extension}")  # Temporary filename
                    with open(temp_filename, 'wb') as audio_file:
                        audio_file.write(audio_response.content)  # Save content to file
                    
                    # Convert to MP3 and downsample to 16000 Hz
                    audio = AudioSegment.from_file(temp_filename, format=extension)
                    audio = audio.set_frame_rate(16000)  # Downsample to 16000 Hz
                    mp3_filename = os.path.join(output_path, f"{video_id}.mp3")  # Final MP3 filename
                    audio.export(mp3_filename, format="mp3", parameters=["-ar", "16000"])  # Export as MP3
                    
                    os.remove(temp_filename)  # Remove the temporary file
                    return {"status": "success", "file": output_filename, "metadata": metadata}  # Return True on successful download and conversion
        # If all attempts fail or no successful download occurs
        open(fail_filename, 'w').close()  # Create .fail file
        return {"status": "failed", "file": output_filename, "metadata": metadata}  # Return False if no successful download occurs
    else:
        print("Error:", response.status_code, response.text)
        open(fail_filename, 'w').close()  # Create .fail file
        return {"status": "failed", "file": output_filename, "metadata": metadata}

# Example usage
# status = download_audio(target_url)
# print(status)

def download_audio_yt_dlp(metadata, output_path):
    # metadata, output_path = args
    # metadata = json.loads(metadata)
    video_id = metadata['id']
    # print(f'Start download {video_id}')
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    output_filename = os.path.join(output_path, f"{video_id}.mp3")
    # print(output_filename)
    if os.path.exists(output_filename):
        # print(f'{output_filename} already exists')
        return {"status": "success", "file": output_filename, "metadata": metadata}

    max_retries = 3
    for attempt in range(max_retries):
        download_command = [
        'yt-dlp',
        '-x',
        '--audio-format', 'mp3',
        '--audio-quality', '5',
        '--postprocessor-args', "ffmpeg:-ar 16000",
        '-o', output_filename,
        '-4', 
        '-q',
        '--no-warnings',
        '--username', 'oauth2', '--password', '',
        metadata['url'],
    ]
        status = subprocess.run(download_command)
        if os.path.exists(output_filename):
            # assert os.path.exists(output_filename)
            # print('Download {} sussessfully with retry tolerance {}'.format(output_filename, attempt))
            return {"status": "success", "file": output_filename, "metadata": metadata}
        else:
            if attempt < max_retries - 1:  # Avoid sleep after the last attempt
                print(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(1)  # Wait for 5 seconds before retrying
    # print('Fail to download {}'.format(output_filename))
    return {"status": "failed", "file": output_filename, "metadata": metadata}

