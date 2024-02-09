#!/bin/bash

# Create a new tmux session named "debuggers"
tmux new-session -d -s debuggers

# Define the Python scripts to execute
scripts=("workers/subdomain_worker.py" "workers/dns_worker.py" "workers/http_worker.py" )
# Loop through the scripts and open them in separate tmux windows
for script in "${scripts[@]}"; do
    tmux new-window -t debuggers: -n "$script" "python3 $script"
done

# Attach to the "debuggers" session to view the output
tmux attach -t debuggers