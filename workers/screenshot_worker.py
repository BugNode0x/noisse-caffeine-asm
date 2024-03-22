import json
import subprocess
import os
import ray
import boto3
import time
import hashlib
from datetime import datetime
from urllib.parse import urlparse
from brain.base_worker import BaseWorker
from brain.db_processor import insert_screenshot_data

@ray.remote
class ScreenshotWorker(BaseWorker):
    bucket_name = 'noisse-shots'

    def upload_to_s3(self, filepath, object_name):
        s3_client = boto3.client('s3')
        try:
            s3_client.upload_file(filepath, self.bucket_name, object_name)
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            return False
        else:
            os.remove(filepath)  # Remove file only after successful upload
            return True


    def generate_timestamped_filename(self, url):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return url.replace("://", "_") + f"_{timestamp}.png"


    def process_task(self, task):
        url = task['url']
        root_domain = task['root_domain']
        user_id = task['user_id']

        self.increment_task_count(root_domain, user_id)

        user_message = f"[Screenshot] Starting screenshot processing for URL: {url}"
        admin_message = f"{user_message} - User ID: {user_id}"
        self.send_slack_notification(user_id, user_message)
        self.send_admin_slack_notification(admin_message)

        # gowitness command execution
        gowitness_cmd = [
            "gowitness", "single", url, 
            "--disable-db", "--disable-logging", 
            "-P", "/root/noisse/noisse-caffeine-asm/screenshots"
        ]

        try:
            subprocess.run(gowitness_cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error taking screenshot with gowitness for {url}: {e}")
            return

        # Construct screenshot filename
        parsed_url = urlparse(url)
        screenshot_filename = f"{parsed_url.scheme}-{parsed_url.netloc}.png"
        screenshot_path = os.path.join("/root/noisse/noisse-caffeine-asm/screenshots", screenshot_filename)

        if not os.path.exists(screenshot_path):
            print(f"Screenshot file not found for URL: {url}")
            return

        # Upload to S3
        object_name = f"{root_domain}/{screenshot_filename}"
        upload_success = self.upload_to_s3(screenshot_path, object_name)
        if not upload_success:
            print(f"Failed to upload screenshot to S3 for URL: {url}")
            return

        # S3 URL construction
        s3_https_base_url = "https://noisse-shots.s3.us-east-2.amazonaws.com/"
        s3_url = f"{s3_https_base_url}{object_name}"

        # Fetching the DOM
        curl_cmd = ['curl', '-X', 'GET', '-H', 'Accept: */*', '-H', 'Accept-Language: en', url]
        try:
            curl_process = subprocess.Popen(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            dom_data_bytes, _ = curl_process.communicate()
            dom_data = dom_data_bytes.decode('utf-8') if curl_process.returncode == 0 else ''

            if upload_success:
                s3_url = f"{s3_https_base_url}{object_name}"
                subdomain = urlparse(url).netloc
                insert_screenshot_data.remote(subdomain, url, s3_url, dom_data)

        except Exception as e:
            print(f"Error processing screenshot for {url}: {e}")

        finally:
            if self.decrement_task_count(root_domain, user_id):
                completion_message = f"Screenshot processing completed for {root_domain}"
                self.push_notification_to_queue(user_id, completion_message)
    
    def increment_task_count(self, root_domain, user_id):
        # Use Redis to increment the task count for the given root_domain and user_id
        redis_key = f"crawl_task_count:{root_domain}:{user_id}"
        self.redis_client.incr(redis_key)
        # Optional: Set a reasonable expiry time for the key
        self.redis_client.expire(redis_key, 4000)  # 4000 seconds expiry time

    def decrement_task_count(self, root_domain, user_id):
        # Use Redis to decrement the task count and check if it reaches zero
        redis_key = f"crawl_task_count:{root_domain}:{user_id}"
        remaining_tasks = self.redis_client.decr(redis_key)
        return remaining_tasks <= 0  # Returns True if all tasks are processed
    
    def push_notification_to_queue(self, user_id, message):
        notification_task = json.dumps({'user_id': user_id, 'message': message})
        self.redis_client.rpush('notification_queue', notification_task)

    def increment_task_count(self, root_domain, user_id):
        # Use Redis to increment the task count for the given root_domain and user_id
        redis_key = f"screenshot_task_count:{root_domain}:{user_id}"
        self.redis_client.incr(redis_key)
        # Optional: Set a reasonable expiry time for the key
        self.redis_client.expire(redis_key, 4000)  # 4000 seconds expiry time

    def decrement_task_count(self, root_domain, user_id):
        # Use Redis to decrement the task count and check if it reaches zero
        redis_key = f"screenshot_task_count:{root_domain}:{user_id}"
        remaining_tasks = self.redis_client.decr(redis_key)
        return remaining_tasks <= 0  # Returns True if all tasks are processed
    
    def push_notification_to_queue(self, user_id, message):
        notification_task = json.dumps({'user_id': user_id, 'message': message})
        self.redis_client.rpush('notification_queue', notification_task)

    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    _, task_json = task_data
                    if task_json:
                        self.process_task(task_json)
        except KeyboardInterrupt:
            print("Shutting down ShotWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 4
    screenshot_workers = [ScreenshotWorker.remote(queue_names=['screenshot_queue']) for _ in range(num_workers)]
    
    for i in range(num_workers):
        worker = ScreenshotWorker.remote(queue_names=[f'screenshot_queue_{i}'])
        worker.run.remote()

    try:
        print("Screenshot Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down Screenshot Workers gracefully...")