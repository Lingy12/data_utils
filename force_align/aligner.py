import fire
from pathlib import Path
import stable_whisper
from nltk.tokenize import sent_tokenize,word_tokenize
import string
import json


model = stable_whisper.load_model('base')

def clean_sent(sent):
    return sent.lower().translate(str.maketrans('', '', string.punctuation))

def refine_script(word_segs, transcript, speaker_map):
    sentences = sent_tokenize(transcript)
    
    sentence_timestamps = []

    word_index = 0
    speaker_sentence_idx = 0
    curr_speaker = speaker_map[speaker_sentence_idx][0]
    speaker_seg = ""
    idx = 0

    for sentence in sentences:
        curr_str = ""
        curr_start = word_segs[word_index].start
        while word_index < len(word_segs) and clean_sent(sentence).strip() != clean_sent(curr_str).strip():
            curr_str += word_segs[word_index].word
            word_index += 1
        curr_end = word_segs[word_index].start if word_index < len(word_segs) else word_segs[-1].end
        
        sentence_timestamps.append({
            "start": curr_start,
            "end": curr_end,
            "sentence": sentence, 
            "speaker": curr_speaker,
            "segment_id": idx + 1
            })
        
        # after appending, check whether need to move to next
        speaker_seg += sentence + ' '
        if clean_sent(speaker_seg.strip().replace(' ', '')) == clean_sent(speaker_map[speaker_sentence_idx][1].strip().replace(' ', '')):
            speaker_seg = ""
            speaker_sentence_idx += 1
            curr_speaker = speaker_map[speaker_sentence_idx][0] if speaker_sentence_idx < len(speaker_map) else None
        
        idx += 1

    if len(sentence_timestamps) != len(sentences):
        raise Exception('Some segment are not able to match original scripts.')

    return sentence_timestamps

def convert_to_txt(file):
    scripts = []
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            scripts.append(json.loads(line))
    
    
    text = ' '.join([s["utterance"] for s in scripts])
    speaker_map = [(s['speaker'], s['utterance']) for s in scripts]
    return text, speaker_map

def force_align(audio, transcript_jsonl):
    transcripts, speaker_map = convert_to_txt(transcript_jsonl)
    result = model.align(audio, transcripts, language='en')
    
    word_segs = [w for seg in result.segments for w in seg.words]
    
    sentence_timestamp = refine_script(word_segs, transcripts, speaker_map)
    
    output_path = Path(audio).with_suffix('.algined.jsonl')

    with open(output_path, 'w') as f:
        for seg in sentence_timestamp:
            f.write(json.dumps(seg) + '\n')

if __name__ == "__main__":
    fire.Fire(force_align)
