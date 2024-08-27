#!/bin/bash

PYTHON_COMMAND="python batch_submit_jobs.py"

while true; do
    echo "Starting Python command: $PYTHON_COMMAND"
    $PYTHON_COMMAND
    echo "Python command exited. Restarting..."
    sleep 1  # Optional: add a short delay before restarting
done
