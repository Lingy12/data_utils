import os
from datasets import Audio, DatasetDict, Dataset
import fire
from tqdm import tqdm

splits = ['train', 'test', 'dev']
transcription_file = 'transcripts.txt'
audio_folder = 'audio'

def read_txt_dict(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    res = {}
    for line in lines:
        id, sentence = line.split('\t', 1)
        sentence = sentence.strip()
        res[id] = sentence
    return res

def build_ds(folder, output_folder):
    ds = DatasetDict()
    for split in tqdm(splits):
        split_folder = os.path.join(folder, split)
        split_audio_folder = os.path.join(split_folder, audio_folder)

        ds_dict = {"id": [], "speaker_id": [], "audio": [], "sentence": [], "segment_id": [], "book_id": []}
        transcription = read_txt_dict(os.path.join(folder, split, transcription_file))
        # print(transcription)
        for id in tqdm(list(transcription.keys())):
            speaker_id, book_id, segment_id = id.split('_')
            audio = os.path.join(split_audio_folder, speaker_id, book_id, f'{id}.flac')
            sentence = transcription[id]
            # print(audio)
            
            for col in ds_dict.keys():
                ds_dict[col].append(eval(col))
        # print(ds_dict['id'])
        split_ds = Dataset.from_dict(ds_dict).cast_column('audio', Audio())
        ds[split] = split_ds

    ds.save_to_disk(output_folder, num_proc=32)

    

if __name__ == "__main__":
    fire.Fire(build_ds)
