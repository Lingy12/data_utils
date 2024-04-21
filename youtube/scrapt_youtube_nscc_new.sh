channel=$1
output_path=$2
num_of_node=$3  # At most 15

# Ensure num_of_node does not exceed 15
if [ "$num_of_node" -gt 15 ]; then
  echo "num_of_node cannot be greater than 15"
  exit 1
fi

name=$(basename "$output_path")
echo $name

mkdir -p "$output_path/log"

# Fetch metadata and capture the job ID
metadata_job_name="${name}_metadata"
metadata_job_id=$(qsub -v channel="$channel",output_path="$output_path" \
     -N "$metadata_job_name" -o "$output_path/log/${metadata_job_name}.log" \
     fetch_meta_wrapper.sh)

echo $metadata_job_id
# Loop from 0 to num_of_node-1 to process nodes
for ((i=0; i<num_of_node; i++)); do
    echo "Scheduling download for node $i on channel $channel"
    qsub -W depend=afterok:$metadata_job_id -v channel="$channel",output_path="$output_path",local_rank="$i",num_of_node="$num_of_node",name="$name" \
         -N "$name.$i" -o "$output_path/log/$name.$i.log" download_wrapper.sh 
done
