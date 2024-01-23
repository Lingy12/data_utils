import fire
import multiprocessing as mp
from typing import Dict, List, Tuple
from tqdm import tqdm
from glob import glob
import textgrid
import os
import re
from datasets import Dataset, Audio
import shutil
from pydub import AudioSegment

TIME_FORMAT ="{:.2f}"
def _segment(args): # speaker level output folder wav_file, seg_lst, output_folder = args
    wav_file, seg_lst, output_folder = args
    wav_file = wav_file.strip()
    failed_lst = []
    if wav_file[-3:] != 'wav':
        print(wav_file[-4:])
        raise Exception('Please check audio format to be wav ' + wav_file)
    audio = AudioSegment.from_wav(wav_file)
        
    for seg in seg_lst:
        id, start, end, script = seg
        curr_seg = audio[int(start) * 10:int(end) * 10]
        seg_name = '-'.join([id, start, end])
        trans_path, wav_path = os.path.join(output_folder, seg_name + '.txt'), os.path.join(output_folder, seg_name + '.wav')
        if len(curr_seg) == 0: failed_lst.append(seg_name)
        else:
            with open(trans_path, 'w') as f:
                f.write(script)
            curr_seg.export(wav_path, format='wav')
    return failed_lst

def _generate_dict_for_speaker(args):
    directory = args
    speaker_dict = {"audio":[], "utt_id":[], "speaker":[], "sentence": []}

    audios = glob(os.path.join(directory, '*.wav')) 
    # print(directory, audios)
    for audio in audios:
        script_file = audio[:-4] + '.txt'
        speaker = audio.split('/')[-2]
        utt_id = audio[:-4]
        with open(script_file, 'r') as f:
            sentence = f.read()
        for attr in speaker_dict.keys():
            speaker_dict[attr].append(eval(attr))
    return speaker_dict

class DataPipeline:
    def __init__(self, transcription_suffix:str, 
                 audio_suffix:str, 
                 transcription_dir:str, 
                 audio_dir: str, 
                 transcription_pattern:str, 
                 audio_pattern:str) -> None:
        self.transcription_suffix = transcription_suffix
        self.audio_suffix = audio_suffix

        self.transcription_dir = transcription_dir
        self.audio_dir = audio_dir

        self.transcription_pattern = rf'{self.transcription_dir}/{transcription_pattern}'
        self.audio_pattern = rf'{self.audio_dir}/{audio_pattern}'

    def _regex_to_glob(self, regex_pattern:str):
        # Replace named groups with *
        glob_pattern = re.sub(r'\(\?P<[^>]+>[^)]+\)', '*', regex_pattern)
        
        # Replace regex-specific patterns
        glob_pattern = glob_pattern.replace('\d', '*').replace('\.', '.')
        
        return glob_pattern

    def _extract_from_path(self, pattern:str, path:str):
        # Regular expression pattern to match the given path pattern
        match = re.match(pattern, path)
        
        if match:
            return match.groupdict()  # Returns a dictionary with named groups
        else:
            raise Exception('Path pattern error')

    def _transform_number(self, num:float, length:int=8):
        # Remove the decimal point
        num_2 = TIME_FORMAT.format(num)
        num_str = str(num_2).replace('.', '')
        
        # Pad with zeros
        padded_num_str = num_str.zfill(length)
        
        return padded_num_str

    def _generate_seg_text_utt2spk(self):
        # print(TRANSCRIPTION_PATTERN)
        transcription_glob = self._regex_to_glob(self.transcription_pattern)
        seg_lst, text_lst, utt2spk_lst = [], [], []
        for tg in tqdm(glob(transcription_glob)):
            # print(tg)
            try:
                grids = textgrid.TextGrid.fromFile(tg)
            except:
                # print(tg)
                raise Exception(f'Check file {tg}')
            speaker = self._extract_from_path(self.transcription_pattern, tg)['speaker']
            for grid in grids[0]:
                # print(grid)
                start, end, sentence = float(TIME_FORMAT.format(grid.minTime)), float(TIME_FORMAT.format(grid.maxTime)), grid.mark
                segment_id = '-'.join([speaker, self._transform_number(start), self._transform_number(end)])
                if len(sentence) == 0 or sentence == '<S>' or sentence == '<Z>':
                    continue
                seg_lst.append((segment_id, speaker, start, end))
                text_lst.append((segment_id, sentence))
                utt2spk_lst.append((segment_id, speaker))
        return seg_lst, text_lst, utt2spk_lst

    def _generate_wav_index(self):
        audio_glob = self._regex_to_glob(self.audio_pattern)
        wav_lst = []

        for audio in tqdm(glob(audio_glob)):
            speaker = self._extract_from_path(self.audio_pattern, audio)['speaker']
            speaker = speaker.lower()
            wav_lst.append((speaker, audio))
        return wav_lst

    def _write_tuples_to_file(self,data:List[Tuple], filename: str):
        """
        Write a list of tuples to a file.
        
        Parameters:
        - data (list of tuples): The data to write to the file.
        - filename (str): The name of the file where the data will be written.
        """
        with open(filename, 'w') as file:
            for item in data:
                file.write(' '.join(map(str, item)) + '\n') 
    
    def create_indexing(self, dest:str):
        seg_lst, text_lst, utt2_spk_lst = self._generate_seg_text_utt2spk()    
        wav_lst = self._generate_wav_index()

        if not os.path.exists(dest):
            os.mkdir(dest)
        # out_dir = dest

        self._write_tuples_to_file(seg_lst, os.path.join(dest, 'segments'))
        self._write_tuples_to_file(text_lst, os.path.join(dest, 'text'))
        self._write_tuples_to_file(utt2_spk_lst, os.path.join(dest, 'utt2spk'))
        self._write_tuples_to_file(wav_lst, os.path.join(dest, 'wav.scp'))
    
    def _parse_file_to_map(self, file:str) -> Dict[str, str]:
        res = {}
        with open(file, 'r') as f:
            lines = f.readlines()
        for line in lines:
            try:
                key, value = line.split(' ', 1)
            except:
                raise Exception(f'Check file: {file}, line = {line}')
            res[key] = value
        return res 

    def _group_by_speaker(self, transcripts: Dict[str, str]) -> Dict[str, List[Tuple]]:
        res = {}
        # print(transcripts)
        for id in transcripts.keys():
            # print(id)
            script = transcripts[id]
            speaker, start, end = id.split('-')
            if speaker not in res:
                res[speaker] = []
            res[speaker].append((speaker, start, end, script))
        return res 

    def segment_from_manifest(self, manifest_dir:str, segmented_dir:str, num_worker:int=4):
        if os.path.exists(segmented_dir):
            need_rm = input("The folder exists, do you want to remove? (y to remove)")
            if need_rm:
                shutil.rmtree(segmented_dir)
        if not os.path.exists(segmented_dir):
            os.mkdir(segmented_dir)
        failed_lst = []
        # for each participant, create folder and check list
        transcripts = self._parse_file_to_map(os.path.join(manifest_dir, 'text')) # parse transcript
        speaker_map = self._parse_file_to_map(os.path.join(manifest_dir, 'wav.scp'))
        speaker_seg_map = self._group_by_speaker(transcripts)
        params = []
        for speaker in speaker_seg_map.keys():
            speaker_folder = os.path.join(segmented_dir, speaker)
            speaker_wav = speaker_map[speaker.lower()]
            if not os.path.exists(speaker_folder):
                os.mkdir(speaker_folder)
            params.append((speaker_wav, speaker_seg_map[speaker], speaker_folder))
        print('Starting jobs for: ' + manifest_dir)
        with mp.Pool(num_worker) as p:
            results = list(tqdm(p.imap(_segment, params), total = len(params)))
        for res in results:
            failed_lst += res
            
        print('Failed list: ')
        print(failed_lst)


    def build_hf_ds(self, segmented_folder, output_folder, num_workers=4):
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        ds_dict = {"audio":[], "utt_id":[], "speaker":[], "sentence": []}
        
        params = []

        for directory in os.listdir(segmented_folder):
            params.append(os.path.join(segmented_folder, directory))

        with mp.Pool(num_workers) as p:
            r = list(tqdm(p.imap(_generate_dict_for_speaker, params), total=len(params)))
        
        for speaker_dict in r:
            for key in ds_dict.keys():
                ds_dict[key] += speaker_dict[key]

            # print(split_dict)
        ds = Dataset.from_dict(ds_dict)
        ds = ds.cast_column('audio', Audio())
        ds.save_to_disk(output_folder, num_proc=num_workers)
        print('ds saved') 

if __name__ == "__main__":
    fire.Fire(DataPipeline)
