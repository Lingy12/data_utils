from datasets import load_from_disk
import os
import sys

source_directory = sys.argv[1]
output_directory = sys.argv[2]

# Load the dataset from disk
ds = load_from_disk(source_directory)

# Read err.txt to get invalid indices
# current_directory = os.path.dirname(os.path.abspath(__file__))
err_file_path = os.path.join(source_directory, "err.txt")

invalid_indices = set()
with open(err_file_path, "r") as err_file:
    for line in err_file:
        try:
            invalid_index = int(line.strip())
            invalid_indices.add(invalid_index)
        except ValueError:
            print(f"Skipping invalid line in err.txt: {line.strip()}")

# Filter out invalid indices
valid_indices = [i for i in range(len(ds)) if i not in invalid_indices]
filtered_ds = ds.select(valid_indices)

# Save the filtered dataset
filtered_ds.save_to_disk(output_directory, num_proc=16)

print(f"Original dataset size: {len(ds)}")
print(f"Filtered dataset size: {len(filtered_ds)}")
print(f"Removed {len(ds) - len(filtered_ds)} invalid entries")
print(f"Filtered dataset saved to: {output_directory}")
