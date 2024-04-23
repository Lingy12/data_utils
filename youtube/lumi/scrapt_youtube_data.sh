SLURM_JOB_NUM_NODES=$(printenv SLURM_JOB_NUM_NODES)
SLURM_NODEID=$(printenv SLURM_NODEID)


channel=$1
output_path=$2
num_of_node=$3  # At most 15
partition=$4
workers_per_node=$5

module load LUMI/22.06  partition/C
# Ensure num_of_node does not exceed 15
if [ "$num_of_node" -gt 15 ]; then
  echo "num_of_node cannot be greater than 15"
  exit 1
fi

name=$(basename "$output_path")
echo $name

mkdir -p "$output_path/log"

# Check if the metadata.json file exists
if [ ! -f "$output_path/metadata.json" ]; then
    echo "metadata.json does not exist, starting metadata job"

    # Fetch metadata and capture the job ID
    metadata_job_name="${name}_metadata"
    metadata_job_id=$(sbatch --job-name=$name.metadata --output=$output_path/log/metadata.log \
                        --partition=$partition --nodes=1 --ntasks-per-node=1 --cpu-per-task=16 \ 
                        --mem=480G lumi/fetch_meta_wrapper.sh $channel $output_path)

    echo $metadata_job_id

    # Loop from 0 to num_of_node-1 to process nodes
    for ((i=0; i<num_of_node; i++)); do
        echo "Scheduling download for node $i on channel $channel"

        sbatch --job-name=$name.$i --dependency=afterok:$metadata_job_id \
                    --output=$output_path/log/$name.$i.log \
                        --partition=$partition --nodes=$num_of_node \ 
                        --ntasks-per-node=$workers_per_node --cpus-per-task=1 --gpus-per-node=0 \ 
                        --mem=480G bash lumi/download_wrapper.sh \
                        $channel $output_path $num_of_node $workers_per_node
        # qsub -W depend=afterok:$metadata_job_id -v channel="$channel",output_path="$output_path",local_rank="$i",num_of_node="$num_of_node",name="$name" \
        #      -N "$name.$i" -o "$output_path/log/$name.$i.log" nscc/download_wrapper.sh 
    done
else
    echo "metadata.json already exists, skipping metadata job"
    # Loop from 0 to num_of_node-1 to process nodes
    for ((i=0; i<num_of_node; i++)); do
        echo "Scheduling download for node $i on channel $channel"

        sbatch --job-name=$name.$i \
            --output=$output_path/log/$name.$i.log \
                --partition=$partition --nodes=$num_of_node \ 
                --ntasks-per-node=$workers_per_node --cpus-per-task=1 --gpus-per-node=0\ 
                --mem=480G bash lumi/download_wrapper.sh \
                $channel $output_path $num_of_node $workers_per_node
    done
fi
