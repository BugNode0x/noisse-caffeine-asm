#!/bin/bash

# Check if tmux is installed
if ! command -v tmux &> /dev/null
then
    echo "tmux could not be found, please install tmux"
    exit 1
fi

# Define the Python scripts to execute
scripts=("workers/subdomain_worker.py" 
"workers/dns_worker.py" 
"workers/http_worker.py" 
"workers/crawl_worker.py" 
"workers/screenshot_worker.py" )

# Create a new tmux session named "debuggers"
tmux new-session -d -s debuggers

# Loop through the scripts and open them in separate tmux windows
for script in "${scripts[@]}"; do
    tmux new-window -t debuggers: -n "$(basename $script)" "python3 $script"
done

# Attach to the "debuggers" session to view the output
tmux attach -t debuggers
