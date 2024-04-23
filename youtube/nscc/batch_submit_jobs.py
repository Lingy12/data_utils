import fire
from typing import List
import os
import subprocess
import logging
import time

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_job_count(max_jobs=80):
    try:
        # Run qstat and pipe the output to wc -l
        p1 = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(["wc", "-l"], stdin=p1.stdout, stdout=subprocess.PIPE, text=True)
        p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
        output, _ = p2.communicate()
        
        current_jobs = int(output.strip())
        logging.info(f"Currently {current_jobs} jobs are running.")
        return current_jobs < max_jobs
    except ValueError as e:
        logging.error(f"Failed to parse job count: {e}")
        return False
    except Exception as e:
        logging.error(f"Error checking job count: {e}")
        return False

def submit_all_job(job_config_lst: List[str], root_path, num_of_nodes=15):
    total_jobs = 0
    successful_jobs = 0
    failed_jobs = 0

    for job in job_config_lst:
        logging.info(f"Processing job file: {job}")

        with open(job, 'r') as f:
            channels_conf = f.readlines()
        
        for channel in channels_conf[1:]:
            # print(channel)
            if not check_job_count():
                logging.warning("Job submission paused due to max job count reached. Waiting for slots to free up.")
                while not check_job_count():
                    time.sleep(60)  # Wait for 1 minute before checking again
            
            total_jobs += 1
            try:
                url, tag = channel.strip().split(',')
                print(url, tag)
                if tag:
                    output_path = os.path.join(root_path, tag.strip())
                    command = ["bash", "scrapt_youtube_nscc.sh", url.strip(), output_path, str(num_of_nodes)]
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
