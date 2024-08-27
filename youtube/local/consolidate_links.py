import json
import os
import fire

def consolidate_data(root_path):
    dirs = os.listdir(root_path)
    fail_count = 0
    
    result_dict = []
    
    for dir in dirs:
        # print(dir)
        meta_data_path = os.path.join(root_path, dir, 'metadata.json')
        channel = dir
        
        if not os.path.exists(meta_data_path):
            fail_count += 1
            continue
        with open(meta_data_path, 'r') as f:
            data = json.load(f)
            
        for entry in data:
            meta = json.loads(entry)
            url, id = meta['url'], meta['id']
            # result_dict['url'].append(url)
            # result_dict['channel'].append(channel)
            result_dict.append({"url": url, "channel": channel, 'id': id})
    
    with open('local/all_data.json', 'w') as f:
        json.dump(result_dict, f, indent=3)

    print('Failed count = {}'.format(fail_count))
    
if __name__ == '__main__':
    fire.Fire(consolidate_data)