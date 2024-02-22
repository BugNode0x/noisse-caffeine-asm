import json
import subprocess
import ray
import os
from urllib.parse import urlparse, parse_qs
from brain.base_worker import BaseWorker
from brain.db_processor import insert_js_result, execute_db_query

ray.init()

class JavaScriptGatheringWorker(BaseWorker):
    def clean_and_deduplicate_urls(self, urls):
        unique_urls = set()
        seen_param_combinations = set()

        for url in urls:
            url = url.strip()
            parsed_url = urlparse(url)
            netloc = parsed_url.netloc

            if netloc.startswith("www."):
                netloc = netloc[4:]

            path = parsed_url.path.rstrip('/') if parsed_url.path.endswith('/') else parsed_url.path
            base_url = f"{parsed_url.scheme}://{netloc}{path}"

            query_params = parse_qs(parsed_url.query)
            param_keys = tuple(sorted(query_params.keys()))

            url_identifier = (base_url, param_keys)

            if url_identifier not in seen_param_combinations:
                seen_param_combinations.add(url_identifier)
                unique_urls.add(url)

        return unique_urls

    def fetch_eligible_urls(self, root_domain):
        query = '''
            SELECT subdomain_id, url FROM http_results 
            WHERE status_code = 200 AND content_length > 1000
            AND url LIKE %s
        '''
        like_pattern = f'%.{root_domain}'
        print(f"With like_pattern: {like_pattern}")
        print(execute_db_query(query, (like_pattern,)))
        return execute_db_query(query, (like_pattern,))


    def process_url(self, subdomain_id, url):
        katana_path = os.path.expanduser('~/go/bin/katana')
        katana_cmd = [katana_path, '-silent', '-d', '2', '-hl', '-jc', '-c', '50', '-p', '50']

        process = subprocess.Popen(katana_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        stdout, _ = process.communicate(input=url.encode())

        if process.returncode == 0:
            js_urls = stdout.decode().splitlines()
            cleaned_urls = self.clean_and_deduplicate_urls(js_urls)
            for js_url in cleaned_urls:
                insert_future = insert_js_result.remote(subdomain_id, js_url)

    def process_task(self, task):
        root_domain = task.get('root_domain')
        if root_domain:
            urls = self.fetch_eligible_urls(root_domain)
            if urls:  # Check if urls is not None
                for subdomain_id, url in urls:
                    self.process_url(subdomain_id, url)
            else:
                print(f"No eligible URLs found for root domain: {root_domain}")
        
    def run(self):
        while True:
            task_data_str = self.redis_client.blpop('crawl_queue', timeout=5)  # Timeout to cycle through queues
            if task_data_str:
                _, task_json_str = task_data_str
                task = json.loads(task_json_str)
                self.process_task(task)

if __name__ == "__main__":
    worker = JavaScriptGatheringWorker(queue_names=['crawl_queue'])
    worker.run()
