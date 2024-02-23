import subprocess
import json
import ray
import os
import time
from brain.db_processor import get_user_id_from_hunter_id, insert_dns_data_remote, execute_db_query 
from brain.base_worker import BaseWorker

@ray.remote
class DNSWorker(BaseWorker):
    def check_subdomain_id_exists(self, subdomain):
        query = "SELECT subdomain_id FROM subdomains WHERE subdomain = %s"
        subdomain_id = execute_db_query(query, (subdomain,), fetch_one=True)
        return subdomain_id is not None


    def process_task(self, task_data):
        _, task = task_data
        subdomain = task['subdomain']
        root_domain = task['root_domain']
        user_id = task['user_id']

        dnsx_path = os.path.expanduser('~/go/bin/dnsx')
        dnsx_cmd = [dnsx_path, '-silent', '-t', '200', '-json', '-asn', '-wd', root_domain,
                    '-rcode', 'noerror,servfail,refused,nxdomain', 
                    '-r', '8.8.8.8,8.8.4.4,1.1.1.1,9.9.9.9,208.67.222.222,84.200.69.80,64.6.64.6,8.26.56.26,205.171.3.65,134.195.4.2,185.222.222.8.9,76.76.19.19,37.235.1.177,77.88.8.1,94.140.14.140,38.132.106.139,74.82.42.42,76.76.2.0']
        max_retries = 5
        retry_count = 0
        while not self.check_subdomain_id_exists(subdomain) and retry_count < max_retries:
            print(f"Waiting for subdomain ID for {subdomain} to be available...")
            time.sleep(2)  # Wait for 2 seconds before retrying
            retry_count += 1

        if retry_count == max_retries:
            print(f"Subdomain ID for {subdomain} not found after retries, skipping DNS processing.")
            return             

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
            result = ray.get(insert_future)

            self.send_slack_notification(user_id, f"Processed DNS for {subdomain} successfully.")

            resolved_subdomain = dns_data['host']
            http_task = json.dumps({'subdomain': resolved_subdomain, 'root_domain': root_domain, 'user_id': user_id})
            self.redis_client.rpush('http_queue', http_task)
            print(f"Pushed to http_queue: {http_task}")

        except Exception as e:
            print(f"Error processing DNS for {subdomain}: {e}")

        

    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    _, task_json = task_data
                    if task_json:
                        self.process_task(task_data)
        except KeyboardInterrupt:
            print("Shutting down DNSWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 4
    dns_workers = [DNSWorker.remote(queue_names=['dns_queue']) for _ in range(num_workers)]
    
    for worker in dns_workers:
        worker.run.remote()

    try:
        print("DNS Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down DNS Workers gracefully...")