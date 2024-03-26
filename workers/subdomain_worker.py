import json
import subprocess
import shlex
import ray
import time
from brain.db_processor import insert_subdomain_results, ensure_hunter_exists, ensure_domain_exists, get_user_id_from_hunter_id
from brain.base_worker import BaseWorker

@ray.remote
class SubdomainEnumerationWorker(BaseWorker):
    def process_task(self, task_data):
        domain = task_data.get('domain')
        self.hunter_id = task_data.get('user_id')
        user_id = get_user_id_from_hunter_id(self.hunter_id)

        print(f"Received domain: {domain}, Hunter ID: {self.hunter_id}")

        if user_id and domain:
            user_message = f"Thanks for using Noisse! We've started doing recon in {domain}"
            admin_message = f"{user_message} - User ID: {self.hunter_id}"
            self.push_notification_to_queue(user_id, user_message)
            self.send_slack_notification(user_id, user_message)
            self.send_admin_slack_notification(admin_message)

        if not domain or not isinstance(domain, str):
            print(f"Invalid or missing domain in task data: {task_data}")
            return

        print(f"Calling ensure_domain_exists with domain: {domain}, Hunter ID: {self.hunter_id}")

        ensure_hunter_exists(self.hunter_id)
        domain_id = ensure_domain_exists(domain)
        if not domain_id or not self.hunter_id:
            print(f"Error processing task for domain {domain} and hunter {self.hunter_id}")
            return

        print(f"Processing domain: {domain} with ID: {domain_id}")

        print(f"Running subfinder")
        safe_domain = shlex.quote(domain)
        command = f"~/go/bin/subfinder -all -recursive -d {safe_domain}"
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error in subdomain enumeration: {stderr.decode()}")
            return

        subdomains = stdout.decode().splitlines()

        if subdomains:
            insert_subdomain_results(self.hunter_id, domain, subdomains)
            end_message = f"Subdomain enumeration completed for {domain}"
            self.push_notification_to_queue(user_id, end_message)

        for subdomain in subdomains:
            task = json.dumps({'subdomain': subdomain, 'root_domain': domain, 'user_id': user_id})
            self.redis_client.rpush('dns_queue', task)

    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    _, task = task_data
                    if task:
                        self.process_task(task)
        except KeyboardInterrupt:
            print("Shutting down SubdomainWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 10
    subdomain_workers = [SubdomainEnumerationWorker.remote(queue_names=['api_queue']) for _ in range(num_workers)]
    
    for worker in subdomain_workers:
        worker.run.remote()

    try:
        print("Subdomain Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down Subdomain Workers gracefully...")