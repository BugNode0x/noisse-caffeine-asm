import sys
import json
import subprocess
import shlex
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from db_processor import insert_subdomain_results, get_root_domain_id, ensure_hunter_exists, ensure_domain_exists
from base_worker import BaseWorker

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
        return subdomains

    def run(self):
        while True:
            task_data = self.fetch_task()
            if task_data:
                domain = task_data.get('domain')
                hunter_id = task_data.get('user_id')

                # Debug print
                print(f"Received domain: {domain}, Hunter ID: {hunter_id}")

                if not domain or not isinstance(domain, str):
                    print(f"Invalid or missing domain in task data: {task_data}")
                    continue

                # Debug print before calling ensure_domain_exists
                print(f"Calling ensure_domain_exists with domain: {domain}, Hunter ID: {hunter_id}")

                # Ensure the hunter exists and get domain ID
                ensure_hunter_exists(hunter_id)
                domain_id = ensure_domain_exists(domain, hunter_id)
                if not domain_id:
                    print(f"Error: Unable to process domain {domain}")
                    continue
                if not hunter_id:
                    print(f"Error: Unable to process hunter {hunter_id}")
                    continue

                print(f"Processing domain: {domain} with ID: {domain_id}")

                # Proceed to enumerate subdomains
                subdomains = self.process_task(domain)
                if subdomains:
                    insert_subdomain_results(hunter_id, domain, subdomains)



if __name__ == "__main__":
    worker = SubdomainEnumerationWorker()
    worker.run()