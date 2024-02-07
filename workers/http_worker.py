import sys
import subprocess
import json
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
import os
from db_processor import insert_http_data
from base_worker import BaseWorker

class HTTPWorker(BaseWorker):
    def process_task(self, task):
        subdomain = task['subdomain']
        root_domain = task['root_domain']
        user_id = task['user_id']
        
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
            insert_http_data(user_id, subdomain, root_domain, url, title, webserver, tech, status_code, content_length)

        except Exception as e:
            print(f"Error processing HTTP for {subdomain}: {e}")

    def run(self):
        while True:
            task_data = self.fetch_task()
            if task_data:
                queue_name, task_json = task_data
                if task_json:  # Check if task_json is not None
                    self.process_task(task_json)

if __name__ == "__main__":
    worker = HTTPWorker(queue_names=['http_queue'])
    worker.run()
