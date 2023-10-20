import fire
import json

def get_total_hour(json_path):
    with open(json_path, 'r') as f:
        res = json.load(f)

    total_hour = 0
    total_sample = 0

    for key in res.keys():
        total_hour += res[key]['audio_hours']
        total_sample += res[key]['no_of_row']

    print('Total hour = {}'.format(total_hour))
    print('Total sample = {}'.format(total_sample))

if __name__ == "__main__":
    fire.Fire(get_total_hour)
