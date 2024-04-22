channel=$1
output_path=$2
local_rank=$3
num_ranks=$4

python download_channels.py fetch_video_metadata $channel $output_path
python download_channels.py worker_process $output_path --workers 16 --local_rank $local_rank --num_ranks $num_of_node --max_files_in_folder 100