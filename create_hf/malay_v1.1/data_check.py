from datasets import load_from_disk

ds = load_from_disk('/data/geyu/malay/malay_v1.1/ntucs/ntucs_hf')

for entry in ds:
    sentence = entry['sentence']

    if '[' in sentence:
         print(sentence)
