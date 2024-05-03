#!/bin/bash

# Directory containing MP3 files
DIRECTORY=$1

total_duration=0

# Loop through all MP3 files in the directory
for file in "$DIRECTORY"/*.mp3; do
    # Get the duration of each file in seconds using ffprobe
    duration=$(ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$file")
    # Accumulate total duration
    total_duration=$(echo "$total_duration + $duration" | bc)
done

# Convert total duration from seconds to hours
total_hours=$(echo "scale=2; $total_duration / 3600" | bc)

# Output the total duration in hours
echo "Total duration: $total_hours hours"
