from datasets import Audio, load_from_disk, Dataset
from tqdm import tqdm
from datasets.features import Value, Sequence
import fire
import multiprocessing
import textgrid
import os
import pandas as pd
from glob import glob
import pathlib
from pathlib import Path

AUDIO_PREFIX = 'Audio'
SCRIPT_PREFIX = 'Scripts'

def fetch_metadata(root):
    
    metadata = os.path.join(root, 'Documents', "Speakers_(Part_3).XLSX")
    records = pd.read_excel(metadata).fillna('Unknown').to_dict(orient='records')
    
    # build speaker dictionary
    print('Record before build = {}'.format(len(records)))
    record_dict = {d['SCD']: d for d in records}
    print('Record after build dictionary = {}'.format(len(record_dict)))
    return record_dict

def get_transcriptions(root, script_type, mictype, wav_file_path):
    if script_type == 'Same':
        audio_basename = pathlib.Path(os.path.basename(wav_file_path)).with_suffix('')
        script_folder = os.path.join(root, '_'.join([SCRIPT_PREFIX, script_type]))
        conversation_id = str(audio_basename).split('-')[0]
        name1, name2 = f'{conversation_id}-1.TextGrid', f'{conversation_id}-2.TextGrid'
        return {"speaker_1": os.path.join(script_folder, name1), "speaker_2": os.path.join(script_folder, name2)}, conversation_id
    else:
        script_folder = os.path.join(root, '_'.join([SCRIPT_PREFIX, script_type]))
        if mictype == 'IVR':
            # print(wav_file_path.split('/'))
            _, conf, id = wav_file_path.split('/')[-3:]
            id = pathlib.Path(id).with_suffix('')
            conversation_id = conf.split('_')[-1]
        else:
            audio_basename = pathlib.Path(os.path.basename(wav_file_path)).with_suffix('')
            conversation_id = str(audio_basename).split('_')[1]
        transcript_files = glob(os.path.join(script_folder, f'conf_{conversation_id}_{conversation_id}*.TextGrid'))
        if len(transcript_files) == 2:
            return {"speaker_1": transcript_files[0], 'speaker_2': transcript_files[1]}, conversation_id
        else:
            return None, conversation_id

def map_intervals(intervals, path):
    # print(intervals)
    return {"chunks": [{"start": interval.minTime, "end": interval.maxTime, "sentence": interval.mark} for interval in intervals[0]],
            "path": path}

def process_file(args):
    f, root, script_t, mic_t, speaker_metadata_dict = args
    transcription_file, conversation_id = get_transcriptions(root, script_t, mic_t, f)
    data = {
        'audio': f,
        'audio_path': f,
        'metadata': {
            'script_type': script_t,
            'mic_type': mic_t,
            'conversation_id': conversation_id
        }
    }
    if transcription_file:
        speaker_1, speaker_2 = transcription_file['speaker_1'], transcription_file['speaker_2']
        try:
            intervals_1 = map_intervals(textgrid.TextGrid.fromFile(speaker_1), speaker_1)
            data['metadata']["chunk_speaker_1"] = intervals_1
        except Exception as e:
            print(e)
            data['metadata']['chunk_speaker_1'] = None

        try:
            intervals_2 = map_intervals(textgrid.TextGrid.fromFile(speaker_2), speaker_2)
            data['metadata']["chunk_speaker_2"] = intervals_2
        except Exception as e:
            print(e)
            data['metadata']['chunk_speaker_2'] = None

        if script_t == 'Same':
            speaker1_id, speaker2_id = str(Path(os.path.basename(speaker_1)).with_suffix('')), str(Path(os.path.basename(speaker_2)).with_suffix(''))
            print(speaker_metadata_dict.get(speaker1_id, ''))
            data['metadata']['speaker_metadata'] = {
                "speaker_1": speaker_metadata_dict.get(speaker1_id, ''),
                "speaker_2": speaker_metadata_dict.get(speaker2_id, '')
            }
        else:
            data['metadata']['speaker_metadata'] = None
    return data

def build_hf(root, output_path, workers=4):
    speaker_metadata_dict = fetch_metadata(root)
    mic_type = {"Same": ["BoundaryMic", "CloseMic"], "Separate": ["IVR", 'StandingMic']}
    ds_dict = {"audio": [], "audio_path": [], "metadata": []}
    
    for script_t in mic_type:
        for mic_t in mic_type[script_t]:
            relative_folder = '_'.join([AUDIO_PREFIX, script_t, mic_t])
            audio_folder = os.path.join(root, relative_folder)
            wav_files = glob(os.path.join(audio_folder, '**', '*.wav'), recursive=True)

            pool = multiprocessing.Pool(processes=workers)
            tasks = [(f, root, script_t, mic_t, speaker_metadata_dict) for f in wav_files]
            
            for result in tqdm(pool.imap(process_file, tasks), total=len(tasks), desc="Processing Files"):
                ds_dict['audio'].append(result['audio'])
                ds_dict['audio_path'].append(result['audio_path'])
                ds_dict['metadata'].append(result['metadata'])
            
            pool.close()
            pool.join()

    ds = Dataset.from_dict(ds_dict)
    print(ds)
    ds.save_to_disk(output_path, num_proc=workers)
    # print(ds_dict)

    

if __name__ == "__main__":
    fire.Fire(build_hf)
