import subprocess
import time
import logging
import sys

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_script(script_name):
    """
    Start a script using the same Python executable as the current process.
    """
    return subprocess.Popen([sys.executable, script_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def is_process_running(process):
    """
    Check if a process is still running.
    """
    return process.poll() is None

def main():
    # List of script names
    scripts = [
        "workers/subdomain_worker.py", "workers/http_worker.py", "workers/dns_worker.py", "workers/screenshot_worker.py", "workers/crawl_worker.py",
    ]

    # Dictionary to hold script processes
    processes = {}

    # Start all scripts
    for script in scripts:
        processes[script] = start_script(script)
        logging.info(f"Started script: {script}")

    # Main loop to monitor scripts
    while True:
        for script, process in processes.items():
            if not is_process_running(process):
                logging.warning(f"Script {script} has stopped. Restarting...")
                processes[script] = start_script(script)
                logging.info(f"Restarted script: {script}")

        # Wait a bit before checking again
        time.sleep(10)

# Uncomment the following line to run the script
main()