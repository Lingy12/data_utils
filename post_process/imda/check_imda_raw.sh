#!/bin/bash

# Define the directory to search for WAV files
directory_to_search=$1

# Function to check WAV file integrity
check_wav_integrity() {
    local wav_file="$1"
    if ffmpeg -v error -i "$wav_file" -f null - 2>&1; then
        echo "OK: $wav_file"
    else
        echo "ERROR: $wav_file"
    fi
}

# Export the function so it can be used by find
export -f check_wav_integrity

# Find all WAV files and check their integrity
find "$directory_to_search" -type f -name "*.wav" -exec bash -c 'check_wav_integrity "$0"' {} \;
# find "$directory_to_search" -type f -name "*.wav" | parallel check_wav_integrity

