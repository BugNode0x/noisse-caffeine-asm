import subprocess
import json
import os
import ray
import time
import hashlib
from brain.db_processor import insert_http_data
from brain.base_worker import BaseWorker

@ray.remote
class HTTPWorker(BaseWorker):
    def process_task(self, task):
        subdomain = task['subdomain']
        root_domain = task['root_domain']
        user_id = task['user_id']

        self.increment_task_count(root_domain, user_id)

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

            screenshot_queue_index = self.select_queue_index(url)
            screenshot_task_queue = f'screenshot_queue_{screenshot_queue_index}'
            screenshot_task = json.dumps({'url': url, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush(screenshot_task_queue, screenshot_task)
            print(f"Pushed to {screenshot_task_queue}: {screenshot_task}")
    
            crawl_queue_index = self.select_queue_index(url)
            crawl_task_queue = f'crawl_queue_{crawl_queue_index}'
            crawl_task = json.dumps({'url': url, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush(crawl_task_queue, crawl_task)
            print(f"Pushed to {crawl_task_queue}: {crawl_task}")

        except Exception as e:
            print(f"Error processing HTTP for {subdomain}: {e}")

        finally:
            # Decrement task count and check if it's time to send the completion message
            if self.decrement_task_count(root_domain, user_id):
                completion_message = f"HTTP probing completed for {root_domain}"
                self.push_notification_to_queue(user_id, completion_message)

    def increment_task_count(self, root_domain, user_id):
        # Use Redis to increment the task count for the given root_domain and user_id
        redis_key = f"http_task_count:{root_domain}:{user_id}"
        self.redis_client.incr(redis_key)
        # Optional: Set a reasonable expiry time for the key
        self.redis_client.expire(redis_key, 4000)  # 4000 seconds expiry time

    def decrement_task_count(self, root_domain, user_id):
        # Use Redis to decrement the task count and check if it reaches zero
        redis_key = f"http_task_count:{root_domain}:{user_id}"
        remaining_tasks = self.redis_client.decr(redis_key)
        return remaining_tasks <= 0  # Returns True if all tasks are processed
    
    def push_notification_to_queue(self, user_id, message):
        notification_task = json.dumps({'user_id': user_id, 'message': message})
        self.redis_client.rpush('notification_queue', notification_task)

    def select_queue_index(self, identifier):
        # Simple hash-based mechanism to select a queue index
        hash_value = int(hashlib.md5(identifier.encode()).hexdigest(), 16)
        num_queues = 4  # Adjust this based on the total number of screenshot_queues you have
        return hash_value % num_queues
    
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
    for i in range(num_workers):
        worker = HTTPWorker.remote(queue_names=[f'http_queue_{i}'])
        worker.run.remote()

    try:
        print("HTTP Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down HTTP Workers gracefully...")