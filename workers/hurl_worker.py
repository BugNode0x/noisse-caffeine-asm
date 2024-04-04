import json
import subprocess
import tempfile
import os
import time
import ray
from brain.base_worker import BaseWorker
from brain.db_processor import insert_hurl_data

@ray.remote
class HurlWorker(BaseWorker):
    def process_task(self, task):
        root_domain = task['root_domain']  # Adjusted to use 'root_domain'
        user_id = task['user_id']
        worker_type = 'hurl'

        # Notify start of process
        self.send_slack_notification(user_id, f"[Hurl] Starting analysis for domain: {root_domain}")
        self.increment_task_count(root_domain, user_id, worker_type)

        # Creating a temporary file for input domain
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_file.write(root_domain)
            input_file = temp_file.name

        output_directory = "/root/noisse/noisse-caffeine-asm/deving"
        output_file = os.path.join(output_directory, f"{root_domain}.txt")
        command = f"waymore -i {input_file} -oU {output_file} -mode U -from 2015 -f"

        try:
            # Execute the command
            subprocess.run(command, shell=True, check=True)
            # Process the output file here...

            with open(output_file, 'r') as file:
                urls = file.readlines()
                for url in urls:
                    url = url.strip()  # Remove any leading/trailing whitespace
                    insert_hurl_data.remote(root_domain, url) 
            
        except Exception as e:
            print(f"Error in HurlWorker: {e}")

        finally:
            os.remove(input_file)  # Clean up the input file
            os.remove(output_file)  # Clean up the output file

            if self.decrement_task_count(root_domain, user_id, worker_type):
                completion_message = f"Hurl analysis completed for {root_domain}"
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

    num_workers = 5
    hurl_workers = [HurlWorker.remote(queue_names=['hurl_queue']) for _ in range(num_workers)]

    for worker in hurl_workers:
        worker.run.remote()

    try:
        print("Hurl Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)  # Keep the main script running
    except KeyboardInterrupt:
        print("Shutting down Hurl Workers gracefully...")
