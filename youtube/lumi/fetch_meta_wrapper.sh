#!/bin/bash
#SBATCH --job-name=placeholder  # Job name
#SBATCH --output=placeholder # Name of stdout output file
#SBATCH --partition=placeholder  # or ju-standard-g, partition name
#SBATCH --nodes=placeholder             # Total number of nodes 
#SBATCH --ntasks-per-node=placeholder
#SBATCH --cpus-per-task=placeholder
#SBATCH --mem=480G
#SBATCH --time=02:00:00       # Run time (d-hh:mm:ss)

channel=$1
output_path=$2

python download_channels.py fetch_video_metadata $channel $output_path