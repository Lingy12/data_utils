import logging
import torch.multiprocessing as mp
import sys
import fire

root = logging.getLogger()
root.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
root.addHandler(handler)
root.info('I am root')
# root.setLevel(logging.DEBUG)

def log_test(rank):
    logger = logging.getLogger('rank {}'.format(rank))
    logger.setLevel(logging.DEBUG)
    # print(rank)
    logger.info('I am rank {}'.format(rank))

def run(num_proc):
    logger = logging.getLogger('run')
    # logger.setLevel(logging.DEBUG)
    logger.info('I am run')
    mp.spawn(log_test, args=(), join=True, nprocs=num_proc)

if __name__ == "__main__":
    fire.Fire(run)        
