'''
This script transform from original asr dataset format to fusion format
("audio", "text") => {context: {"text": None, "audio": speech array }, 
question: {"text":  "Please transribe bla bla bla ", "audio": None }, 
answer: {"text": Transcription, "audio": None }, other columns...}
'''
from typing import Any
from datasets import load_from_disk, Audio, Features, Value
import fire
from dataclasses import dataclass
import random

instructions_asr = [
    'Please transcribe the speech.',
    'Kindly jot down the speech.',
    'Record the conversation verbatim, please.',
    'Could you write out what was said?',
    'Capture the words spoken accurately.',
    'Transcribe the speech for reference.',
    'Could you document the spoken words precisely?',
    'Please write down the spoken content.',
    'Please provide a written version of the speech.',
    'Could you transcribe the speech accurately.',
    'Create a written record of the spoken communication.',
    'Please write out the speech.',
    'Record the spoken words in text form.',
    'Could you put the speech into writing?',
    'Capture the speech in written format, please.',
    'Transcribe the spoken content, if you will.',
    'Write down what was said, please.',
    'Convert the speech into written text.',
    'Document the conversation by writing it down.',
    'Could you transcribe the verbal exchange?',
    'Please convert the spoken words into text.',
    'Create a written rendition of the speech.',
    'Record the verbal communication in textual form.',
    'Could you provide a written account of the speech?',
    'Transcribe the conversation accurately, please.',
    'Write out the words that were spoken.',
    'Please document the spoken language.',
    'Capture the spoken content in writing.',
    'Convert the speech into a written transcript.',
    'Could you transcribe what you hear?',
    'Please put the spoken words into writing.'
]


@dataclass
class DataMapper(object):
    text_col: str
    audio_col: str
    
    def __call__(self, example) -> Any:
            return {"context": {"text": None,
                        "audio": {"array": example[self.audio_col]["array"], "sampling_rate": example[self.audio_col]["sampling_rate"]}},
            "instruction": {"text": random.choice(instructions_asr),
                            "audio": None},
            "answer": {"text": example[self.text_col],
                        "audio": None},
            "other_attributes": {k: example[k] for k in set(example.keys()) - set([self.text_col, self.audio_col])}
            }

def transform_data(input_ds, output_path, workers=16, audio_col='audio', text_col='sentence'):
    target_ds = load_from_disk(input_ds)
    if 'sentence' not in target_ds.column_names:
        print('sentence not in dataset, change text_col = text')
        text_col = 'text'

    if 'audio' not in target_ds.column_names:
        raise Exception('Not a valid dataset, please ensure column exists')
    
    target_ds = target_ds.cast_column('audio', Audio(sampling_rate=16000))
    print('Resample finished')
    
    mapper = DataMapper(text_col=text_col, audio_col=audio_col)
    
    features = Features({
        'context': {"text": Value(dtype='string'), "audio": Audio(sampling_rate=16000, decode=True)},
        'instruction': {"text": Value(dtype='string'), "audio": Audio(sampling_rate=16000, decode=True)},
        'answer': {"text": Value(dtype='string'), "audio": Audio(sampling_rate=16000, decode=True)},
        'other_attributes': {
           k: target_ds.features[k] for k in set(target_ds.column_names) - set([audio_col, text_col])
        }
    })
    print(features)
    
    target_ds = target_ds.map(mapper, features=features, remove_columns=target_ds.column_names, num_proc=workers)
    target_ds.save_to_disk(output_path, num_proc=workers)
    
if __name__ == "__main__":
    fire.Fire(transform_data)


