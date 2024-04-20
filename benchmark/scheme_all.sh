#!/bin/bash

# Check if the root folder path is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <root_folder>"
    exit 1
fi

# Store the root folder path
root_folder="$1"

# Traverse the root folder and find all subfolders
for subfolder in "$root_folder"/*/; do
    # Ensure it's a directory
    if [ -d "$subfolder" ]; then
        # Remove trailing slash for consistency in naming
        subfolder=${subfolder%/}
        # Extract the name of the subfolder
        subfolder_name=$(basename "$subfolder")
        # Call the Python script with the required arguments
        python ../create_hf/transform_to_fusion_format.py "$subfolder" "${subfolder}.schemed" 1
    fi
done

