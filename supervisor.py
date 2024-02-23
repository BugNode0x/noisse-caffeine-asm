import subprocess
import time
import logging
import sys
import signal
import os
import requests

SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_slack_notification(message):
    payload = {"text": message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending Slack notification: {e}")
        

def start_script(script_name):
    """
    Start a script using the same Python executable as the current process.
    """
    outfile = open(f"{script_name}.log", "a")  # Append mode
    return subprocess.Popen([sys.executable, script_name], stdout=outfile, stderr=outfile)

def is_process_running(process):
    """
    Check if a process is still running.
    """
    return process.poll() is None

def stop_process(process):
    """
    Gracefully stop a process.
    """
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()

def signal_handler(signal, frame):
    logging.info("Received shutdown signal. Stopping all scripts...")
    for process in processes.values():
        stop_process(process)
    sys.exit(0)

def main():
    global processes  # to be accessible in signal_handler

    # List of script names
    scripts = [
        "workers/subdomain_worker.py", 
        "workers/http_worker.py",
        "workers/dns_worker.py", 
        "workers/screenshot_worker.py",
        "workers/crawl_worker.py"
    ]

    # Dictionary to hold script processes
    processes = {script: start_script(script) for script in scripts}

    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start all scripts
    for script in scripts:
        logging.info(f"Started script: {script}")

    # Main loop to monitor scripts
    while True:
        for script, process in list(processes.items()):
            if not is_process_running(process):
                error_message = f"Script {script} has stopped. Restarting..."
                logging.warning(error_message)
                send_slack_notification(error_message)  # Send notification to Slack

                processes[script] = start_script(script)
                restart_message = f"Restarted script: {script}"
                logging.info(restart_message)
                send_slack_notification(restart_message)  # Notify about script restart

        time.sleep(10)   # Wait a bit before checking again

if __name__ == "__main__":
    main()