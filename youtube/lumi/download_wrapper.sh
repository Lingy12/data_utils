#!/bin/bash
#SBATCH --job-name=placeholder  # Job name
#SBATCH --output=placeholder # Name of stdout output file
#SBATCH --partition=placeholder  # or ju-standard-g, partition name
#SBATCH --nodes=placeholder             # Total number of nodes 
#SBATCH --ntasks-per-node=placeholder
#SBATCH --cpus-per-task=placeholder
#SBATCH --mem=480G
#SBATCH --time=02:00:00       # Run time (d-hh:mm:ss)

# Load any necessary modules or environment settings
channel=$1
local_rank=$2
num_of_node=$3
workers=$4

echo "Channel = $channel"
echo "local_rank=$local_rank"
echo "num nodes = $num_of_node" 
echo "workers = $workers"
# Execute the Python script
python download_channels.py worker_process $output_path --workers $workers\
                    --local_rank $local_rank --num_ranks $num_of_node \
                    --max_files_in_folder 1000