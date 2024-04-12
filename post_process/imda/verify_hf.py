from datasets import load_from_disk
import os
import fire
import logging

log_format = '%(asctime)s - %(levelname)s - %(message)s'
# Set up logging
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

def check_ds(data_path, split_size, num_shard):
    ds = load_from_disk(data_path)

    splited_ds = ds.train_test_split(test_size=split_size)
    train_ds, val_ds = splited_ds['train'], splited_ds['test']
    train_ds = train_ds.shuffle(42)
    val_ds = val_ds.shuffle(42)
    logger.info('train_ds: length = {}'.format(len(train_ds)))
    logger.info('val_ds: length = {}'.format(len(val_ds)))
    train_ds = train_ds.to_iterable_dataset(num_shards=num_shard)
    val_ds = val_ds.to_iterable_dataset(num_shards=num_shard)
    logger.info('train_ds: shard = {}'.format(train_ds.n_shards))
    logger.info('val_ds: shard = {}'.format(val_ds.n_shards))
    i = 0
    for entry in train_ds:
        if i >= 256 * 200 and i <= 256 * 225:
            prefix = f'{i} [check]'
        else:
            prefix = f'{i}'
        sentence = entry['sentence']

        logger.info(prefix + ' ' + sentence)
        i += 1
if __name__ == "__main__":
    fire.Fire(check_ds)

