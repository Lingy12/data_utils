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

def download_ds(dest: str, num_worker:int = 4):
    for lang in langs:
        curr_dest = os.path.join(dest, lang)
        
        if os.path.exists(curr_dest):
            print(f'{lang} ds downloaded, skipping')
            continue
        print(f'Downloading and load {lang} dataset from hugging face hub...')
        ds = load_dataset('mozilla-foundation/common_voice_13_0', lang, num_proc=num_worker)
        print('Dataset loaded')
        print(f'Saving dataset to {curr_dest}')
        try:
            ds.save_to_disk(curr_dest, num_proc=num_worker)
            print('Dataset saved.')
        except:
            print('Probably split problem.')


        ds.cleanup_cache_files()
        print('Cache cleaned')

if __name__ == "__main__":
    fire.Fire(download_ds)
