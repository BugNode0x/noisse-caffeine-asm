import json
import subprocess
import shlex
import ray
from brain.db_processor import insert_subdomain_results, ensure_hunter_exists, ensure_domain_exists, get_user_id_from_hunter_id
from brain.base_worker import BaseWorker

@ray.remote
class SubdomainEnumerationWorker(BaseWorker):
    def process_task(self, domain):
        print(f"Running subfinder")
        # Logic to enumerate subdomains using subfinder
        safe_domain = shlex.quote(domain)  # Safely format domain
        command = f"~/go/bin/subfinder  -all -recursive -d {safe_domain}"
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error in subdomain enumeration: {stderr.decode()}")
            return None

        subdomains = stdout.decode().splitlines()
        user_id = get_user_id_from_hunter_id(self.hunter_id)

        for subdomain in subdomains:
            task = json.dumps({'subdomain': subdomain, 'root_domain': domain, 'user_id': user_id})
            self.redis_client.rpush('dns_queue', task)

        return subdomains

    def run(self):
        try:
            while True:
                queue_name, task_data = self.fetch_task()
                if task_data:
                    domain = task_data.get('domain')
                    self.hunter_id = task_data.get('user_id')
                    user_id = get_user_id_from_hunter_id(self.hunter_id)

                    # Debug print
                    print(f"Received domain: {domain}, Hunter ID: {self.hunter_id}")
                    if user_id:
                        self.send_slack_notification(user_id, f"[!] Subdomain gathering started for {domain}.")


                    if not domain or not isinstance(domain, str):
                        print(f"Invalid or missing domain in task data: {task_data}")
                        continue

                    # Debug print before calling ensure_domain_exists
                    print(f"Calling ensure_domain_exists with domain: {domain}, Hunter ID: {self.hunter_id}")

                    # Ensure the hunter exists and get domain ID
                    ensure_hunter_exists(self.hunter_id)
                    domain_id = ensure_domain_exists(domain)
                    if not domain_id or not self.hunter_id:
                        print(f"Error processing task for domain {domain} and hunter {self.hunter_id}")
                        continue

                    print(f"Processing domain: {domain} with ID: {domain_id}")

                    # Proceed to enumerate subdomains
                    subdomains = self.process_task(domain)
                    if subdomains:
                        insert_subdomain_results(self.hunter_id, domain, subdomains)
        except KeyboardInterrupt:
            print("Shutting down SubdomainWorker gracefully...")
                    


if __name__ == "__main__":
    ray.init()

    num_workers = 4
    workers = [SubdomainEnumerationWorker.remote(queue_names=['api_queue']) for _ in range(num_workers)]
    
    for worker in workers:
        worker.run.remote()