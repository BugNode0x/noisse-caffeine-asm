import json
import subprocess
import os
import ray
import base64
import boto3
from urllib.parse import urlparse
from brain.base_worker import BaseWorker
from brain.db_processor import insert_screenshot_data

ray.init()


class ScreenshotWorker(BaseWorker):
    def upload_to_s3(self, filepath, bucket_name, object_name):
        s3_client = boto3.client('s3')
        try:
            s3_client.upload_file(filepath, bucket_name, object_name)
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            return False
        return True

    def process_task(self, task):
        url = task['url']
        root_domain = task['root_domain']
        user_id = task['user_id']
        subdomain = urlparse(url).netloc

        dom_data = None

        # Construct the screenshot filename and path
        screenshot_filename = url.replace("://", "_") + ".png"
        screenshot_path = os.path.join('/home/ubuntu/asm/dev/noisse-caffeine-asm/temp-shots', screenshot_filename)

        
        try:
            nuclei_path = os.path.expanduser('~/go/bin/nuclei')
            nuclei_cmd = [nuclei_path, '-t', '/home/ubuntu/asm/dev/noisse-caffeine-asm/templates/screenshot.yaml', '-headless', '-silent']
            nuclei_process = subprocess.Popen(nuclei_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            _, _ = nuclei_process.communicate(input=url.encode())

            if nuclei_process.returncode == 0:
                bucket_name = 'noisse-shots'
                object_name = os.path.basename(screenshot_path)
                upload_success = self.upload_to_s3(screenshot_path, bucket_name, object_name)
                s3_url = f"s3://{bucket_name}/{object_name}" if upload_success else None

                upload_success = self.upload_to_s3(screenshot_path, bucket_name, object_name)

                if not upload_success:
                    print(f"Failed to upload screenshot: {screenshot_path}")

        except Exception as e:
            print(f"Error taking screenshot for {url}: {e}")

        try:

            curl_cmd = ['curl', '-X', 'GET', '-H', 'Accept: */*', '-H', 'Accept-Language: en', '-H', 'User-Agent: Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36 Edg/88.0.705.81', url]
            curl_process = subprocess.Popen(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            dom_data_bytes, _ = curl_process.communicate()

            if curl_process.returncode == 0:
                dom_data = dom_data_bytes.decode('utf-8')  # Decode bytes to string
        except Exception as e:
            print(f"Error fetching DOM for {url}: {e}")

        insert_screenshot_data.remote(subdomain, url, s3_url, dom_data)

        crawl_task = json.dumps({'root_domain': root_domain, 'user_id': user_id})
        self.redis_client.rpush('crawl_queue', crawl_task)
        print(f"Pushed to crawl_queue: {crawl_task}")

    def run(self):
        while True:
            task_data = self.fetch_task()
            if task_data:
                _, task_json = task_data
                if task_json:
                    self.process_task(task_json)

if __name__ == "__main__":
    worker = ScreenshotWorker(queue_names=['screenshot_queue'])
    worker.run()
