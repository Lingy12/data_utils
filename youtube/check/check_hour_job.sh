#!/bin/bash
#PBS -P 13003826
#PBS -q normal
#PBS -l select=1:ncpus=32:ngpus=0:mem=110gb
#PBS -l walltime=24:00:00
#PBS -o /home/users/astar/ares/lingy/scratch/log/count.log
#PBS -j oe
#PBS -k oed

# Load any necessary modules or environment settings
module load ffmpeg

source /data/projects/13003565/geyu/miniconda3/etc/profile.d/conda.sh 
conda activate base
echo "Virtual environment activated"
echo $PBS_O_WORKDIR

cd $PBS_O_WORKDIR

python check_hours.py /home/project/13003826/geyu/youtube_local_raw 32
