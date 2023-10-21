from datasets import load_dataset
import fire

import os
subset = ['clean', 'clean_sa', 'dirty', 'dirty_sa', 'microset', 'test', 'validation']
def download_ds(dest: str, num_worker:int=4):
    print('Downloading and load dataset from hugging face hub...')
    
    print('Using {} processes'.format(num_worker))

    for target in subset:
        target_path = os.path.join(dest, target)

        if os.path.exists(target_path):
            print('Split exists, continue {}'.format(target))
            continue
        ds = load_dataset('MLCommons/peoples_speech', num_proc=num_worker, name=target)
        print('Dataset loaded')

        print('Saving dataset...')
        ds.save_to_disk(target_path, num_proc=num_worker)
        print('Dataset saved.')

        ds.cleanup_cache_files()
        print('Local cache cleared')

if __name__ == "__main__":
    fire.Fire(download_ds)
