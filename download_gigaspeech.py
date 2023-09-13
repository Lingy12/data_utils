from datasets import load_dataset
import sys

ds = load_dataset("speechcolab/gigaspeech", sys.argv[1], use_auth_token=True, num_proc=int(sys.argv[3]))
ds.save_to_disk(sys.argv[2], num_proc=int(sys.argv[3]))

