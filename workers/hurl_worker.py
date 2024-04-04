import json
import subprocess
import tempfile
import os
import time
import ray
from brain.base_worker import BaseWorker


@ray.remote
class HurlWorker(BaseWorker):
    def process_task(self, task):
        domain = task['domain']
        user_id = task['user_id']
        worker_type = 'hurl'

        # Notify start of process
        self.send_slack_notification(user_id, f"[Hurl] Starting analysis for domain: {domain}")
        self.increment_task_count(domain, user_id, worker_type)

        # Creating a temporary file for input domain
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_file.write(domain)
            input_file = temp_file.name

        output_file = f"{domain}.txt"
        command = f"waymore -i {input_file} -oU {output_file} -mode U -from 2013 -f"

        try:
            # Execute the command
            subprocess.run(command, shell=True, check=True)
            # Process the output file here...
            
        except Exception as e:
            print(f"Error in HurlWorker: {e}")

        finally:
            os.remove(input_file)  # Clean up the input file
            os.remove(output_file)  # Clean up the output file

            if self.decrement_task_count(domain, user_id, worker_type):
                completion_message = f"Hurl analysis completed for {domain}"
                self.push_notification_to_queue(user_id, completion_message)

    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    _, task = task_data
                    if task:
                        self.process_task(task)
        except KeyboardInterrupt:
            print("Shutting down HurlWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 5  # Define the number of worker instances
    hurl_workers = [HurlWorker.remote(queue_names=['api_queue']) for _ in range(num_workers)]

    for worker in hurl_workers:
        worker.run.remote()

    try:
        print("Hurl Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)  # Keep the main script running
    except KeyboardInterrupt:
        print("Shutting down Hurl Workers gracefully...")
