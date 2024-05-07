import torch
import whisper
import multiprocessing

# torch.set_num_threads(1)

# from IPython.display import Audio
# from pprint import pprint
import librosa

USE_ONNX = False 

vad_models = dict()
whisper_models = dict()
vad_utils = dict()

def init_vad_model():
    pid = multiprocessing.current_process().pid
    model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                                model='silero_vad',
                                force_reload=False,
                                onnx=False)
    vad_models[pid] = model.to('cuda')
    vad_utils[pid] = utils
    print(f'vad model initialized for pid = {pid}')
    
def init_whisper_model():
    pid = multiprocessing.current_process().pid
    whisper_model = whisper.load_model("base")
    whisper_models[pid] = whisper_model
    
    print(f'whisper model initialized for pid = {pid}')

# ## loading model
# model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
#                               model='silero_vad',
#                               force_reload=True,
#                               onnx=USE_ONNX)

# (get_speech_timestamps,
#  save_audio,
#  read_audio,
#  VADIterator,
#  collect_chunks) = utils

def vad_seg(audio_file, output_file):
    pid = multiprocessing.current_process().pid
    (get_speech_timestamps,
 save_audio,
 read_audio,
 VADIterator,
 collect_chunks) = vad_utils[pid]
    sr = librosa.get_samplerate(audio_file)
    sound = read_audio(audio_file, sampling_rate=sr)
    speech_timestamps = get_speech_timestamps(sound, vad_models[pid], sampling_rate=sr)
    # pprint(speech_timestamps)
    save_audio(output_file,
           collect_chunks(speech_timestamps, sound), sampling_rate=sr) 

def get_va_length(audio_file):
    pid = multiprocessing.current_process().pid
    (get_speech_timestamps,
    save_audio,
    read_audio,
    VADIterator,
    collect_chunks) = vad_utils[pid]
    sr = librosa.get_samplerate(audio_file)
    sound = read_audio(audio_file, sampling_rate=sr)
    speech_timestamps = get_speech_timestamps(sound.to('cuda'), vad_models[pid], sampling_rate=sr)
    va_length = sum([ts['end'] - ts['start'] for ts in speech_timestamps])
    
    return {"active_length": va_length / sr, "original_length": len(sound) / sr, "active_ratio": va_length / len(sound)}

def identify_language(audio_file):
    pid = multiprocessing.current_process().pid
    # assumed a vad file
    audio = whisper.load_audio(audio_file)
    audio = whisper.pad_or_trim(audio)
    
    mel = whisper.log_mel_spectrogram(audio).to(whisper_models[pid].device)
    _, probs = whisper_models[pid].detect_language(mel)
    
    sorted_dict = {k: v for k, v in sorted(probs.items(), key=lambda item: item[1], reverse=True)}

    # Get the top 10 items
    top_10 = dict(list(sorted_dict.items())[:10])
    return top_10
    