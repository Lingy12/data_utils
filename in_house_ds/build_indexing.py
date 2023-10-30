import fire
from tqdm import tqdm
from glob import glob
import pathlib
import textgrid
import os
import re
TRANSCRIPTION_SUFFIX='.TextGrid'
AUDIO_SUFFIX = '.wav'

TRANSCRIPTION_DIR = 'Manual_Transcripts'
DATA_DIR = 'i-Data'

TRANSCRIPTION_PATTERN = rf'{os.getcwd()}/{TRANSCRIPTION_DIR}/(?P<speaker>[^/]+).TextGrid'
AUDIO_PATTERN = rf'{os.getcwd()}/{DATA_DIR}/(?P<level>[^/]+)/(?P<split>[^/]+)/(?P<speaker>[^/]+).wav'

TIME_FORMAT = "{:.2f}"

def regex_to_glob(regex_pattern):
    # Replace named groups with *
    glob_pattern = re.sub(r'\(\?P<[^>]+>[^)]+\)', '*', regex_pattern)
    
    # Replace regex-specific patterns
    glob_pattern = glob_pattern.replace('\d', '*').replace('\.', '.')
    
    return glob_pattern

def extract_from_path(pattern, path):
    # Regular expression pattern to match the given path pattern
    match = re.match(pattern, path)
    
    if match:
        return match.groupdict()  # Returns a dictionary with named groups
    else:
        raise Exception('Path pattern error')

def transform_number(num, length=8):
    # Remove the decimal point
    # num = round(num, 2)
    num_str = str(num).replace('.', '')
    
    # Pad with zeros
    padded_num_str = num_str.zfill(length)
    
    return padded_num_str

def generate_seg_text_utt2spk():
    # print(TRANSCRIPTION_PATTERN)
    transcription_glob = regex_to_glob(TRANSCRIPTION_PATTERN)
    seg_lst, text_lst, utt2spk_lst = [], [], []
    for tg in tqdm(glob(transcription_glob)):
        # print(tg)
        grids = textgrid.TextGrid.fromFile(tg)
        speaker = extract_from_path(TRANSCRIPTION_PATTERN, tg)['speaker']
        for grid in grids[0]:
            # print(grid)
            start, end, sentence = TIME_FORMAT.format(grid.minTime), TIME_FORMAT.format(grid.maxTime), grid.mark
            segment_id = '-'.join([speaker, transform_number(start), transform_number(end)])
            seg_lst.append((segment_id, speaker, start, end))
            text_lst.append((segment_id, sentence))
            utt2spk_lst.append((segment_id, speaker))
    return seg_lst, text_lst, utt2spk_lst

def generate_wav_index():
    audio_glob = regex_to_glob(AUDIO_PATTERN)
    wav_lst = []

    for audio in tqdm(glob(audio_glob)):
        speaker = extract_from_path(AUDIO_PATTERN, audio)['speaker']
        speaker = speaker.lower()
        wav_lst.append((speaker, audio))
    return wav_lst

def write_tuples_to_file(data, filename):
    """
    Write a list of tuples to a file.
    
    Parameters:
    - data (list of tuples): The data to write to the file.
    - filename (str): The name of the file where the data will be written.
    """
    with open(filename, 'w') as file:
        for item in data:
            file.write(' '.join(map(str, item)) + '\n') 

seg_lst, text_lst, utt2_spk_lst = generate_seg_text_utt2spk()    
wav_lst = generate_wav_index()

if not os.path.exists('indexing'):
    os.mkdir('indexing')
out_dir = 'indexing'

write_tuples_to_file(seg_lst, os.path.join(out_dir, 'segments'))
write_tuples_to_file(text_lst, os.path.join(out_dir, 'text'))
write_tuples_to_file(utt2_spk_lst, os.path.join(out_dir, 'utt2spk'))
write_tuples_to_file(wav_lst, os.path.join(out_dir, 'wav.scp'))


