#!/bin/bash
#PBS -P 13003565
#PBS -q normal
#PBS -l select=1:ncpus=16:ngpus=0:mem=110gb
#PBS -l walltime=24:00:00
#PBS -j oe
#PBS -k oed

# Load any necessary modules or environment settings
module load ffmpeg

source /data/projects/13003565/geyu/miniconda3/etc/profile.d/conda.sh 
conda activate base
echo "Virtual environment activated"

cd /home/project/13003826/geyu/data_utils/youtube

echo "Channel = $channel"
echo "local_rank=$local_rank"
echo "num nodes = $num_of_node" 
# Execute the Python script
python download_channels.py $channel $output_path --workers 16 --local_rank $local_rank --num_ranks $num_of_node