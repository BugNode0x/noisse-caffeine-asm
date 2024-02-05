# test_subdomain_worker.py
from subdomain_enumerator_worker import SubdomainEnumerationWorker

def test_subdomain_worker(domain):
    worker = SubdomainEnumerationWorker()
    subdomains = worker.process_task(domain)
    return subdomains

test_domain = "vulnweb.com"
subdomains = test_subdomain_worker(test_domain)

if subdomains is not None:
    print(f"Subdomains of {test_domain}:")
    for subdomain in subdomains:
        print(subdomain)
else:
    print("Error: Subdomain enumeration failed or subfinder not found.")
