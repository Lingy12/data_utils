import fire
from glob import glob
import whisper
import torch.multiprocessing as mp
import os
from tqdm import tqdm
import numpy as np
import torch
from datetime import datetime

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def get_rank_lst(batched_wav_lst, rank, word_size):
    N = len(batched_wav_lst)
    start = int(N / word_size) * rank
    end = int(N / word_size) * (rank + 1)
    if rank == word_size - 1:
        return batched_wav_lst[start:]
    else:
        return batched_wav_lst[start : end]

    

def inference_on_device(rank, word_size, model_size, batched_wavs, devices, ori_folder, output_folder, language):
    rank_wavs = get_rank_lst(batched_wavs, rank, word_size)
    torch.cuda.set_device(devices[rank])
    device = 'cuda'
    model = whisper.load_model(model_size).to(device)
    tqdm_title = '# rank {} inference'.format(rank)

    with tqdm(total=len(rank_wavs), desc=tqdm_title, position=rank + 1) as pbar:
        for wav_batch in rank_wavs:
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
            pbar.update(1)
         
def transcribe_audios(ori_folder, output_folder, whisper_size, language, devices=[0], batch_size=4):

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
    batched_wavs = list(batch(wavs, batch_size))
    # print(batched_wavs)
    print('total batch = {}'.format(len(batched_wavs)))

    print('Start spawning process')

    mp.spawn(inference_on_device, args=(len(devices), whisper_size, batched_wavs, devices, ori_folder, output_folder, language)
             , nprocs=len(devices), join=True) 
if __name__ == "__main__":
    fire.Fire(transcribe_audios)
