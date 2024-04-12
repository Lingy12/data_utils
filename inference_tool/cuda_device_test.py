import torch.multiprocessing as mp
import torch
import fire

def run_rank(rank, devices):
    prefix = '# rank {} :'.format(rank)
    torch.cuda.set_device(devices[rank])
    print(prefix + 'cuda current device is ' + str(torch.cuda.current_device()))


def run(devices=[0,1]):
    mp.spawn(run_rank, args=(devices, ), nprocs=len(devices), join=True)


if __name__ == "__main__":
    fire.Fire(run)
