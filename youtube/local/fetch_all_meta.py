import fire
import sys
sys.path.append('../')
from typing import List
import os
import subprocess
import logging
import time
from glob import glob
# from aggregate_download_status import aggregate_counts

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def remove_files_recursively(start_path, pattern):
#     # Search for files matching the pattern recursively
#     for filename in glob(os.path.join(start_path, '**', pattern), recursive=True):
#         # Remove each file found
#         os.remove(filename)
#         logging.info(f'Removed: {filename}')
        
# def check_job_count(max_jobs=80):
#     try:
#         # Run qstat and pipe the output to wc -l
#         p1 = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
#         p2 = subprocess.Popen(["wc", "-l"], stdin=p1.stdout, stdout=subprocess.PIPE, text=True)
#         p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
#         output, _ = p2.communicate()
        
#         current_jobs = int(output.strip())
#         logging.info(f"Currently {current_jobs} jobs are running.")
#         return current_jobs < max_jobs
#     except ValueError as e:
#         logging.error(f"Failed to parse job count: {e}")
#         return False
#     except Exception as e:
#         logging.error(f"Error checking job count: {e}")
#         return False

def submit_all_job(job_config_lst: List[str] = ['nscc/vidiq.job', 'nscc/hyper_audit.job'], root_path='/home/geyu/data/youtube_local_raw'):
    print(job_config_lst)
    total_jobs = 0
    successful_jobs = 0
    failed_jobs = 0
    batch_start = time.time()
    submitted_folder = []
    
    logging.info('Check oauth2')
    commands = ['yt-dlp', '-F', 'https://www.youtube.com/watch?v=2tM1LFFxeKg', '--username', 'oauth2', '--password', '']
    runres = subprocess.run(commands)
    if runres.returncode != 0:
        logging.error('Cannot auth')
        return
    logging.info('Auth2 passed')
    for job in job_config_lst:
        logging.info(f"Processing job file: {job}")

        with open(job, 'r') as f:
            channels_conf = f.readlines()
        
        for channel in channels_conf[1:]:
            try:
                url, tag = channel.strip().split(',')
                print(url, tag)
                if tag:
                    output_path = os.path.join(root_path, tag.strip())
                    
                    if os.path.exists(os.path.join(output_path, 'metadata.json')):
                        logging.info('Already exists for {}'.format(tag))
                        continue
                        
                    # curr_count = aggregate_counts(output_path)
                    # print(output_path)
                    # if curr_count['fail_count'] + curr_count['success_count'] > 10 and (curr_count['fail_count'] < 10 or curr_count['fail_count'] < 0.1 * curr_count['success_count']):
                    #     logging.info('Number of failing = {}'.format(curr_count['fail_count']))
                    #     logging.info('The folder already be downloaded. We could skip this. (not more than 10 failing)')
                    #     continue
                        
                    # logging.warning('Resubmitting... Removing previous count.')
                    # remove_files_recursively(output_path, 'count*.json')
                    submitted_folder.append(output_path)
                    command = ["bash", "local/fetch_meta.sh", url.strip(), output_path]
                    start_time = time.time()
                    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
                    duration = time.time() - start_time
                    logging.info(f"Successfully ran {command} in {duration:.2f} seconds")
                    successful_jobs += 1
                else:
                    logging.warning('Channel line does not have a valid tag, ignored.')
            except subprocess.CalledProcessError as e:
                logging.error(f"Error running job with url {url}: {e.stderr.decode()}")
                failed_jobs += 1
            except Exception as e:
                logging.error(f"Unexpected error for url {url}: {e}")
                failed_jobs += 1

    logging.info(f"Total jobs: {total_jobs}, Successful jobs: {successful_jobs}, Failed jobs: {failed_jobs}")

if __name__ == '__main__':
    fire.Fire(submit_all_job)
