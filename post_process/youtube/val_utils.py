import torch
import whisper

whisper_model = whisper.load_model("base")
torch.set_num_threads(1)

# from IPython.display import Audio
# from pprint import pprint
import librosa

USE_ONNX = False 

## loading model
model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                              model='silero_vad',
                              force_reload=True,
                              onnx=USE_ONNX)

(get_speech_timestamps,
 save_audio,
 read_audio,
 VADIterator,
 collect_chunks) = utils

def vad_seg(audio_file, output_file):
    sr = librosa.get_samplerate(audio_file)
    sound = read_audio(audio_file, sampling_rate=sr)
    speech_timestamps = get_speech_timestamps(sound, model, sampling_rate=sr)
    # pprint(speech_timestamps)
    save_audio(output_file,
           collect_chunks(speech_timestamps, sound), sampling_rate=sr) 

def get_va_length(audio_file):
    sr = librosa.get_samplerate(audio_file)
    sound = read_audio(audio_file, sampling_rate=sr)
    speech_timestamps = get_speech_timestamps(sound, model, sampling_rate=sr)
    va_length = sum([ts['end'] - ts['start'] for ts in speech_timestamps])
    
    return {"active_length": va_length / sr, "original_length": len(sound) / sr, "active_ratio": va_length / len(sound)}

def identify_language(audio_file):
    # assumed a vad file
    audio = whisper.load_audio(audio_file)
    audio = whisper.pad_or_trim(audio)
    
    mel = whisper.log_mel_spectrogram(audio).to(whisper_model.device)
    _, probs = whisper_model.detect_language(mel)
    
    sorted_dict = {k: v for k, v in sorted(probs.items(), key=lambda item: item[1], reverse=True)}

    # Get the top 10 items
    top_10 = dict(list(sorted_dict.items())[:10])
    return top_10
    