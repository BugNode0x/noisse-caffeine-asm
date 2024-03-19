import subprocess
import json
import os
import ray
import time
from brain.db_processor import insert_http_data
from brain.base_worker import BaseWorker

@ray.remote
class HTTPWorker(BaseWorker):
    def process_task(self, task):
        subdomain = task['subdomain']
        root_domain = task['root_domain']
        user_id = task['user_id']

        user_message = f"[HTTP] Starting HTTP probing for subdomain: {subdomain}"
        admin_message = f"{user_message} - User ID: {user_id}"
        self.send_slack_notification(user_id, user_message)
        self.send_admin_slack_notification(admin_message)
        
        httpx_path = os.path.expanduser('~/go/bin/httpx')
        httpx_cmd = [httpx_path, '-silent', '-tech-detect', '-json']
        try:
            process = subprocess.Popen(httpx_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            stdout, _ = process.communicate(input=subdomain.encode())

            if process.returncode != 0:
                raise Exception(f"httpx failed with exit code {process.returncode}")

            http_data = json.loads(stdout.decode())
            print(http_data)
            # Extract necessary data from http_data
            url = http_data['url']
            title = http_data.get('title', '')
            webserver = http_data.get('webserver', '')
            tech = ', '.join(http_data.get('tech', []))
            status_code = http_data.get('status_code', 0)
            content_length = http_data.get('content_length', 0)

            # Call function to insert data into the database
            insert_future = insert_http_data.remote(user_id, subdomain, root_domain, url, title, webserver, tech, status_code, content_length)

            screenshot_task = json.dumps({'url': url, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush('screenshot_queue', screenshot_task)
            print(f"Pushed to screenshot_queue: {screenshot_task}")

        except Exception as e:
            print(f"Error processing HTTP for {subdomain}: {e}")

    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    queue_name, task_json = task_data
                    if task_json:  # Check if task_json is not None
                        self.process_task(task_json)
        except KeyboardInterrupt:
            print("Shutting down HTTPWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 4
    http_workers = [HTTPWorker.remote(queue_names=['http_queue']) for _ in range(num_workers)]
    
    for worker in http_workers:
        worker.run.remote()

    try:
        print("HTTP Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down HTTP Workers gracefully...")