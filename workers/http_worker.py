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
        worker_type = 'http'

        self.increment_task_count(root_domain, user_id, worker_type)

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

            screenshot_queue_index = self.select_queue_index(f"{user_id}_{url}")  # Use a unique identifier
            screenshot_task_queue = f'screenshot_queue_{screenshot_queue_index}'
            screenshot_task = json.dumps({'url': url, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush(screenshot_task_queue, screenshot_task)
            print(f"Pushed to {screenshot_task_queue}: {screenshot_task}")
    
            crawl_queue_index = self.select_queue_index(f"{user_id}_{url}")  # Use a unique identifier
            crawl_task_queue = f'crawl_queue_{crawl_queue_index}'
            crawl_task = json.dumps({'url': url, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush(crawl_task_queue, crawl_task)
            print(f"Pushed to {crawl_task_queue}: {crawl_task}")

        except Exception as e:
            print(f"Error processing HTTP for {subdomain}: {e}")

        finally:
            # Decrement task count and check if it's time to send the completion message
            if self.decrement_task_count(root_domain, user_id, worker_type):
                completion_message = f"HTTP probing completed for {root_domain}"
                self.push_notification_to_queue(user_id, completion_message)
                
    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    queue_name, task = task_data
                    if task:
                        self.process_task(task)
        except KeyboardInterrupt:
            print("Shutting down HTTPWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 5
    http_workers = [HTTPWorker.remote(queue_names=[f'http_queue_{i}']) for i in range(num_workers)]

    for worker in http_workers:
        worker.run.remote()

    try:
        print("HTTP Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down HTTP Workers gracefully...")