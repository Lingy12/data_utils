import torch
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
    pprint(speech_timestamps)
    save_audio('only_speech_test.wav',
           collect_chunks(speech_timestamps, sound), sampling_rate=sr) 

