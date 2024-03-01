import fire
from datasets import load_from_disk, Dataset, Audio
import multiprocessing as mp
import os
from pydub import AudioSegment
from pathlib import Path
from tqdm import tqdm

def list_wav(directory):
    wav_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.wav'):
                wav_files.append(os.path.join(root, file))
    return wav_files

def segment_audio(args):
    file, output_path = args

    audio = AudioSegment.from_wav(file)
    count = 0
    # print(len(audio))
    try:
        for i in range(0, len(audio),30000):
            segment = audio[i:i+30000]
            target_path = os.path.join(output_path, str(Path(os.path.basename(file)).with_suffix('')) + f'_{count}.wav')
            print(len(segment))
            if len(segment) > 0:
                segment.export(target_path, format='wav')
            count += 1
    except:
        return 1

    return 0


def segment_audios(directory, output_path, workers):
    print('segmenting audio...')
    wavs = list_wav(directory)
    
    params = [(file, output_path) for file in wavs]
    with mp.Pool(workers) as p:
        r = list(tqdm(p.imap(segment_audio, params), total = len(params)))
    
    fail_count = 0
    for res in r:
        if res == 1:
            fail_count += 1
    print(f'{fail_count} of the audios cannot be segmented properly')

    print('segmented audio into ' + output_path)

def build_hf_ds(segmented_dir, final_hf_path, workers):
    wavs = list_wav(segmented_dir)
    data_dict = {"name": [], 'audio': []}
    for file in wavs:
        name = str(Path(os.path.basename(file)).with_suffix(''))
        audio = file

        for k in data_dict:
            data_dict[k].append(eval(k))
    
    ds = Dataset.from_dict(data_dict)
    ds = ds.cast_column('audio', Audio())

    ds.save_to_disk(final_hf_path, num_proc=workers)


def segment_and_build_df(input_dir, final_hf_path, workers):
    seg_dir = os.path.join(final_hf_path, 'segmented')
    hf_path = os.path.join(final_hf_path, 'hf')
    
    os.makedirs(final_hf_path, exist_ok=True)
    os.makedirs(seg_dir, exist_ok=True)

    segment_audios(input_dir, seg_dir, workers)
    build_hf_ds(seg_dir, hf_path, workers)

    print('built successfully')

if __name__ == "__main__":
    fire.Fire(segment_and_build_df)
