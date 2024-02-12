import json
import subprocess
import os
import ray
import base64
from urllib.parse import urlparse
from brain.base_worker import BaseWorker
from brain.db_processor import insert_screenshot_data

ray.init()


class ScreenshotWorker(BaseWorker):
    def encode_image_to_base64(self, filepath):
        with open(filepath, 'rb') as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        os.remove(filepath)  
        return encoded_string

    def process_task(self, task):
        url = task['url']
        root_domain = task['root_domain']
        user_id = task['user_id']
        
        subdomain = urlparse(url).netloc

        screenshot_filename = url.replace("://", "_") + ".png"
        screenshot_path = os.path.join('/home/ubuntu/asm/dev/noisse-caffeine-asm/temp-shots', screenshot_filename)

        
        try:
            nuclei_path = os.path.expanduser('~/go/bin/nuclei')
            nuclei_cmd = [nuclei_path, '-t', '/home/ubuntu/asm/dev/noisse-caffeine-asm/templates/screenshot.yaml', '-headless', '-silent']
            nuclei_process = subprocess.Popen(nuclei_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            _, _ = nuclei_process.communicate(input=url.encode())

            if nuclei_process.returncode != 0:
                raise Exception(f"Nuclei failed with exit code {nuclei_process.returncode}")

            base64_screenshot = self.encode_image_to_base64(screenshot_path)

            curl_cmd = ['curl', '-X', 'GET', '-H', 'Accept: */*', '-H', 'Accept-Language: en', '-H', 'User-Agent: Mozilla/5.0 ...', url]
            curl_process = subprocess.Popen(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            dom_data, _ = curl_process.communicate()

            if curl_process.returncode != 0:
                raise Exception(f"Curl failed with exit code {curl_process.returncode}")

            # Store screenshot and DOM data in the database
            insert_screenshot_data.remote(subdomain, url, base64_screenshot, dom_data)


        except Exception as e:
            print(f"Error taking screenshot for {url}: {e}")

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
