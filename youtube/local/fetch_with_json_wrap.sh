#!/bin/bash
#PBS -P 13003826
#PBS -q normal
#PBS -l select=1:ncpus=8:ngpus=0:mem=110gb
#PBS -l walltime=24:00:00
#PBS -j oe
#PBS -k oed

# Load any necessary modules or environment settings
# device_index=$1
# total_device=$2


module load ffmpeg

source /data/projects/13003565/geyu/miniconda3/etc/profile.d/conda.sh 
conda activate base
echo "Virtual environment activated"
echo $PBS_O_WORKDIR
cd $PBS_O_WORKDIR
export PYTHONPATH=$PYTHONPATH:$PBS_O_WORKDIR
# Execute the Python script
# python download_channels.py fetch_video_metadata $channel $output_path
python local/download_with_json.py ./local/all_data.json /home/project/13003826/geyu/youtube_local_raw $total_device $device_index
