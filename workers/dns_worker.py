import sys
import subprocess
import json
from pathlib import Path
import os
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from db_processor import insert_dns_data, get_user_id_from_hunter_id
from base_worker import BaseWorker

class DNSWorker(BaseWorker):
    def process_task(self, task_data):
        _, task = task_data  # task is already a dictionary
        subdomain = task['subdomain']
        root_domain = task['root_domain']
        user_id = task['user_id']

        # Expand the tilde to the full path of the user's home directory
        dnsx_path = os.path.expanduser('~/go/bin/dnsx')

        dnsx_cmd = [
            dnsx_path, '-silent', '-t', '200', '-json', '-asn', '-wd', root_domain,
            '-rcode', 'noerror,servfail,refused,nxdomain', 
            '-r', '8.8.8.8,8.8.4.4,1.1.1.1,9.9.9.9,208.67.222.222,84.200.69.80,64.6.64.6,8.26.56.26,205.171.3.65,134.195.4.2,185.222.222.8.9,76.76.19.19,37.235.1.177,77.88.8.1,94.140.14.140,38.132.106.139,74.82.42.42,76.76.2.0'
        ]

        try:
            process = subprocess.Popen(dnsx_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            stdout, _ = process.communicate(input=subdomain.encode())

            if process.returncode != 0:
                raise Exception(f"dnsx failed with exit code {process.returncode}")

            dns_data = json.loads(stdout.decode())
            print(dns_data)

            insert_dns_data(dns_data, user_id)  # Implement this in db_processor.py
            self.send_slack_notification(user_id, f"Processed DNS for {subdomain} successfully.")
        except Exception as e:
            print(f"Error processing DNS for {subdomain}: {e}")
            self.send_slack_notification(user_id, f"Error processing DNS for {subdomain}: {e}")

    def run(self):
        while True:
            task_data = self.fetch_task()  # No argument is passed
            if task_data:
                _, task_json = task_data
                if task_json is not None:  # Check if task_json is not None
                    self.process_task(task_data)


if __name__ == "__main__":
    worker = DNSWorker(queue_names=['dns_queue'])
    worker.run()
