import fire
from glob import glob
import whisper
import os
from tqdm import tqdm
import multiprocessing as mp         
import numpy as np
import torch
from datetime import datetime

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def transcribe_audios(ori_folder, output_folder, whipser_size, language, device='cuda:0', batch_size=4):

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
        
    wavs = []
    for dir, sub_dirs, files in os.walk(ori_folder):
        output_tgt = os.path.join(output_folder, os.path.relpath(dir, ori_folder))
        if not os.path.exists(output_tgt):
            os.mkdir(output_tgt)
        if len(sub_dirs) == 0: # leave containing audios
            audio_files = glob(os.path.join(dir, '*.wav'))
            wavs += audio_files
    print(len(wavs))
    # up to here, directory is created 

    model = whisper.load_model(whipser_size)
    batched_wavs = list(batch(wavs, batch_size))
    # print(batched_wavs)
    print('total batch = {}'.format(len(batched_wavs)))
    for wav_batch in tqdm(batched_wavs):
        output_lst, mels = [], []
        for wav in wav_batch:
            relative_path = os.path.relpath(wav, ori_folder)
            output_txt_path = os.path.join(output_folder, relative_path[:-4] + '.txt')
            output_lst.append(output_txt_path)
            audio = whisper.load_audio(wav)
            audio = whisper.pad_or_trim(audio)
            mel = whisper.log_mel_spectrogram(audio).unsqueeze(0)
            mels.append(mel)
        with torch.no_grad():
            mels = torch.cat(mels, dim = 0).to(device)
            options = whisper.DecodingOptions(language=language)
            results = whisper.decode(model, mels, options)
            for i in range(len(results)):
                with open(output_lst[i], 'w') as f:
                    f.write(results[i].text)
        
if __name__ == "__main__":
    fire.Fire(transcribe_audios)
