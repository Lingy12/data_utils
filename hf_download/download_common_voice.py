from datasets import load_dataset
import fire
import os

langs = ['bn','de', 'en', 'fa', 'fr', 'es', 'sl', 'kab', 'cy', 'ca', 'tt', 
         'ta', 'ru', 'nl', 'it', 'eu', 'tr', 'ar', 'zh-TW', 'br', 'pt', 'eo', 
         'zh-CN', 'id', 'ia', 'lv', 'ja', 'rw', 'sv-SE', 'cnh', 'et', 'ky', 
         'ro', 'hsb', 'el', 'cs', 'pl', 'rm-sursilv', 'rm-vallader', 'mn', 
         'zh-HK', 'ab', 'cv', 'uk', 'mt', 'as', 'ka', 'fy-NL', 'dv', 'pa-IN', 
         'vi', 'or', 'ga-IE', 'fi', 'hu', 'th', 'lt', 'lg', 'hi', 'bas', 'sk',
         'kmr', 'bg', 'kk', 'ba', 'gl', 'ug', 'hy-AM', 'be', 'ur', 'gn', 'sr', 
         'uz', 'mr', 'da', 'myv', 'nn-NO', 'ha', 'ckb', 'ml', 'mdf', 'sw', 'sat', 
         'tig', 'ig', 'nan-tw', 'mhr','tok', 'yue', 'sah', 'mk', 'sc', 'skr', 'ti',
         'mrj', 'tw', 'ko', 
         'yo', 'oc', 'tk', 'vot', 'az', 'ast', 'ne-NP', 'quy', 'lo', 'dyu', 'is']  

splits = ['train', 'validation', 'test', 'other', 'invalidated', 'dev']

def download_ds(dest: str, num_worker:int = 4):
    for lang in langs:
        curr_dest = os.path.join(dest, lang)
        
        print(f'Downloading and load {lang} dataset from hugging face hub...')

        for split in splits:
            saving_dest = os.path.join(curr_dest, split)
            print(saving_dest)
            if os.path.exists(saving_dest) and len(os.listdir(saving_dest)) > 0:
                print(f'{lang}/{split} ds downloaded, skipping')
                continue
            
            try:
                ds = load_dataset('mozilla-foundation/common_voice_13_0', lang, num_proc=num_worker, split=split)
            except:
                print(f'{lang}/{split} does not exist. Please check the dataset')
                continue
            print('Dataset loaded')
            print(f'Saving dataset to {lang}/{saving_dest}')
            try:
                ds.save_to_disk(saving_dest, num_proc=num_worker)
                print('Dataset saved.')
            except:
                print(f'Saving {lang}/{split} failed.')


            ds.cleanup_cache_files()
            print('Cache cleaned')

if __name__ == "__main__":
    fire.Fire(download_ds)
