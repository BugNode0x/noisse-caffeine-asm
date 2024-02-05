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
        # Here you will implement the logic to enumerate subdomains using subfinder
        safe_domain = shlex.quote(domain)  # Ensures the domain is safely formatted
        command = f"~/go/bin/subfinder -silent -all -recursive -d {safe_domain}"
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            # Handle any errors that occurred during the subprocess
            print(f"Error in subdomain enumeration: {stderr.decode()}")
            return None

        subdomains = stdout.decode().splitlines()
        return subdomains

    def run(self):
        while True:
            task_data = self.fetch_task()
            if task_data:
                task = json.loads(task_data[1])
                domain = task['domain']
                hunter_id = task['user_id']  # Directly use hunter_id from task
    
                # Ensure the hunter and domain exist in the database
                ensure_hunter_exists(hunter_id)
                ensure_domain_exists(domain, hunter_id)  # Pass hunter_id here
    
                # Now fetch the root_domain_id
                root_domain_id = get_root_domain_id(domain)
                if root_domain_id is None:
                    print(f"Error: Unable to find root_domain_id for domain {domain} after insertion.")
                    continue
    
                subdomains = self.process_task(domain)
                
                if subdomains:
                    insert_subdomain_results(hunter_id, root_domain_id, subdomains)

if __name__ == "__main__":
    worker = SubdomainEnumerationWorker()
    worker.run()