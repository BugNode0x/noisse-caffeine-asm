import subprocess
import json
import ray
import os
from brain.db_processor import get_user_id_from_hunter_id, insert_dns_data_remote
from brain.base_worker import BaseWorker

ray.init()

class DNSWorker(BaseWorker):
    def process_task(self, task_data):
        _, task = task_data
        subdomain = task['subdomain']
        root_domain = task['root_domain']
        user_id = task['user_id']

        dnsx_path = os.path.expanduser('~/go/bin/dnsx')
        dnsx_cmd = [dnsx_path, '-silent', '-t', '200', '-json', '-asn', '-wd', root_domain,
                    '-rcode', 'noerror,servfail,refused,nxdomain', 
                    '-r', '8.8.8.8,8.8.4.4,1.1.1.1,9.9.9.9,208.67.222.222,84.200.69.80,64.6.64.6,8.26.56.26,205.171.3.65,134.195.4.2,185.222.222.8.9,76.76.19.19,37.235.1.177,77.88.8.1,94.140.14.140,38.132.106.139,74.82.42.42,76.76.2.0']

        try:
            process = subprocess.Popen(dnsx_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            stdout, _ = process.communicate(input=subdomain.encode())

            if process.returncode != 0:
                raise Exception(f"dnsx failed with exit code {process.returncode}")

            dns_data = json.loads(stdout.decode())
            print(dns_data)

            # Insert DNS data asynchronously
            insert_future = insert_dns_data_remote.remote(dns_data)

            # Optional: wait for the operation to complete
            # result = ray.get(insert_future)

            self.send_slack_notification(user_id, f"Processed DNS for {subdomain} successfully.")

            resolved_subdomain = dns_data['host']
            http_task = json.dumps({'subdomain': resolved_subdomain, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush('http_queue', http_task)
            print(f"Pushed to http_queue: {http_task}")

        except Exception as e:
            print(f"Error processing DNS for {subdomain}: {e}")

    def run(self):
        while True:
            task_data = self.fetch_task()
            if task_data:
                _, task_json = task_data
                if task_json is not None:
                    self.process_task(task_data)

if __name__ == "__main__":
    worker = DNSWorker(queue_names=['dns_queue'])
    worker.run()
