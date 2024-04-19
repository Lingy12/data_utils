channel=$1
output_path=$2
num_of_node=$3 # at most 15

# Ensuring num_of_node does not exceed 15
if [ "$num_of_node" -gt 15 ]; then
  echo "num_of_node cannot be greater than 15"
  exit 1
fi

name=$(basename $output_path)
echo $name

mkdir -p "$output_path/log"

# Loop from 0 to num_of_node
for ((i=0; i<num_of_node; i++)); do
    echo "Processing node $i on channel $channel"

    qsub -v channel="$channel",output_path="$output_path",local_rank="$i",num_of_node="$num_of_node",name="$name" \
         -N $name.$i -o $output_path/log/$name.$i.log download_wrapper.sh 
done