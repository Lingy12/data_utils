import torch.multiprocessing as mp
from datasets import load_from_disk
from transformers import WhisperProcessor, WhisperForConditionalGeneration
from datasets import Audio, load_dataset
import fire
import numpy as np
import torch
from math import ceil
from tqdm import tqdm
from evaluate import load

def get_rank_ds(rank, word_size, ds):
    N = len(ds)
    start = int(N / word_size) * rank
    end = int(N / word_size) * (rank + 1)
    indices = list(range(N))
    if rank == word_size - 1:
        return ds.select(indices[start:])
    else:
        return ds.select(indices[start:end])

def batch_process(processor, audio_batch):
    features = []
    for audio in audio_batch:
        feature = processor(audio['array'], sampling_rate=audio['sampling_rate'])['input_features']
        features.append(feature)
    return {"input_features": features}

def run_rank(rank, word_size, model_name, ds, devices, batch_size, language, shared_dict):
    tqdm_title = '# rank {} transcription: '.format(rank)
    rank_ds = get_rank_ds(rank, word_size, ds)
    torch.cuda.set_device(devices[rank])
    batched_iter = rank_ds.iter(batch_size)
    processor = WhisperProcessor.from_pretrained(model_name)
    print('Number of sample = {}'.format(len(rank_ds)))
    model = WhisperForConditionalGeneration.from_pretrained(model_name)
    forced_decoder_ids = processor.get_decoder_prompt_ids(language=language, task='transcribe')
    res = []
    for batch in tqdm(batched_iter, desc=tqdm_title, position=rank):
        audio = batch['audio']
        print(audio)
        # input_features = processor(audio["array"], sampling_rate=audio["sampling_rate"])
        batch_feature = batch_process(processor, audio)
        # print(batch_feature['input_features'])
        
        with torch.no_grad():
            predicted_ids = model.generate(batch_feature.to('cuda'))
        transcriptions = processor.batch_decode(predicted_ids, forced_decoder_ids=forced_decoder_ids)
        res.extend(transcriptions)
    
    shared_dict[rank] = res

def run(ds_path, batch_size,model_name,language,local=True, devices=[0,1]):
    if local:
        ds = load_from_disk(ds_path)
    else:
        ds = load_dataset(ds_path)
    
    with mp.Manager() as manager:
        res_dict = manager.dict()
        mp.spawn(run_rank, args=(len(devices), model_name, ds.copy(), devices, batch_size, language, res_dict))
    
    # aggregate result
    transcription_lst = [res_dict[i] for i in range(devices)]
    ds.add_column(name='transcribed', column=transcription_lst) 

    wer = load('wer')
    wer_score = 100 * wer.compute(references=ds['sentence'], predictions=ds['transcribed'])
    print('WER score for dataset {} is {}'.format(ds_path, wer_score))
    
if __name__ == "__main__":
    fire.Fire(run)

