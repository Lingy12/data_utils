import torch
import fire
import os
from tqdm import tqdm
from pathlib import Path
from glob import glob
import whisper

device = 'cpu'
torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
model = whisper.load_model("large-v3",device=device)

def transcribe(audio_file):
    result = model.transcribe(audio_file, language='en', condition_on_previous_text=False)

    return result['text']

def output_dict(file, obj):
    with open(file, 'w') as f:
        for k in obj.keys():
            f.write(f'{k} {obj[k]}\n')

def transcribe_folder(folder):
    print('start transcribing: ' + folder)
    audio_folder = folder
    all_audios = glob(os.path.join(audio_folder, '*.wav'))

    res_dict = {}

    for audio in tqdm(all_audios):
        transcription = transcribe(audio)

        res_dict[os.path.basename(Path(audio).with_suffix(''))] = transcription
        with open(Path(audio).with_suffix('.txt'), 'w') as f:
            f.write(transcription)
    
    output_dict(os.path.join(folder, 'text.transcribed'), res_dict)



if __name__ == '__main__':
    fire.Fire(transcribe_folder)
