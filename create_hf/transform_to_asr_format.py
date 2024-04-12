'''
This script transform from original asr dataset format to fusion format
{context: {"text": None, "audio": speech array }, 
question: {"text":  "Please transribe bla bla bla ", "audio": None }, 
answer: {"text": Transcription, "audio": None }, other columns...} => (audio, text)
'''



from dataclasses import dataclass
from datasets import load_from_disk, Audio, Value, Features
import fire

@dataclass
class RevertDataMapper(object):
    text_col: str
    audio_col: str
    
    def __call__(self, example):
        reverted_example = {
            self.text_col: example["answer"]["text"],
            self.audio_col: example["context"]["audio"]
        }
        # Include other attributes as separate columns
        for key, value in example["other_attributes"].items():
            reverted_example[key] = value
        return reverted_example

def revert_transformed_data(input_ds, output_path, workers=16, audio_col='audio', text_col='sentence'):
    transformed_ds = load_from_disk(input_ds)

    if 'context' not in transformed_ds.column_names or 'answer' not in transformed_ds.column_names:
        raise Exception('Not a valid dataset, please ensure it has been transformed using DataMapper')

    revert_mapper = RevertDataMapper(text_col=text_col, audio_col=audio_col)
    
    # Determine features for the reverted dataset
    features = {
        text_col: Value(dtype='string'),
        audio_col: Audio(sampling_rate=16000, decode=True)
    }
    for key in transformed_ds.features['other_attributes']:
        features[key] = transformed_ds.features['other_attributes'][key]

    features = Features(features)
    print(features)
    
    reverted_ds = transformed_ds.map(revert_mapper, features=features, remove_columns=transformed_ds.column_names, num_proc=workers)
    reverted_ds.save_to_disk(output_path, num_proc=workers)

if __name__ == "__main__":
    fire.Fire(revert_transformed_data)
