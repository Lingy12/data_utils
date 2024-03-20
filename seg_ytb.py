import os
from pydub import AudioSegment
from multiprocessing import Pool
from tqdm import tqdm
import glob
import fire

import os
from pydub import AudioSegment
from multiprocessing import Pool

def process_subfolder(args):
    subdir, output_root, segment_duration_ms = args
    output_subdir = os.path.join(output_root, os.path.join(os.path.basename(subdir)))
    if not os.path.exists(output_subdir):
        os.makedirs(output_subdir)

    total_duration_ms = 0
    for file in tqdm(sorted(os.listdir(subdir))):
        if file.endswith('.wav'):
            file_path = os.path.join(subdir, file)
            audio = AudioSegment.from_file(file_path, format='mp4')

            length_audio = len(audio)
            start = 0
            end = segment_duration_ms
            count = 0
            while start < length_audio:
                segment = audio[start:end]
                segment_file_name = f'{os.path.splitext(file)[0]}_segment_{count}.wav'
                segment.export(os.path.join(output_subdir, segment_file_name), format='wav')
                total_duration_ms += len(segment)
                count += 1
                start += segment_duration_ms
                end += segment_duration_ms

    return total_duration_ms

def split_wav_into_segments(input_root, output_root, workers, segment_duration_ms=30000):
    args_list = []
    for subdir, subsubdir, file in os.walk(input_root):
        if subdir != input_root or subsubdir is None:  # Skip the root directory itself and leave
            args_list.append((subdir, output_root, segment_duration_ms))

    print(len(args_list))
    
    with Pool(workers) as pool:
        durations = list(tqdm(pool.imap_unordered(process_subfolder, args_list), total=len(args_list)))
        total_duration_ms = sum(durations)

    total_hours = total_duration_ms / (1000 * 60 * 60)
    print(f'Total duration: {total_hours} hours')

if __name__ == '__main__':
    fire.Fire(split_wav_into_segments)