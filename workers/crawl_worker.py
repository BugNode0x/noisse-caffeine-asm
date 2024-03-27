import json
import subprocess
import ray
import os
import time
from urllib.parse import urlparse, parse_qs
from brain.base_worker import BaseWorker
from brain.db_processor import insert_js_result, execute_db_query

@ray.remote
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
        katana_cmd = [katana_path, '-u', url, '-silent', '-d', '2', '-hl', '-jc', '-c', '50', '-p', '50', '-scp', '/snap/bin/chromium', '--no-sandbox', '-ef', 'ttf,woff,svg,png,gif,jpg,css,jpeg,ico,woff2,eot']

        print(f"Executing Katana command: {' '.join(katana_cmd)}")
        process = subprocess.Popen(katana_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = process.communicate()

        if process.returncode == 0:
            print(f"Katana command executed successfully for URL: {url}")
            js_urls = stdout.decode().splitlines()
            cleaned_urls = self.clean_and_deduplicate_urls(js_urls)
            print(f"Cleaned URLs for {url}: {cleaned_urls}")
            for js_url in cleaned_urls:
                print(f"Inserting JS URL into DB: {js_url}")
                insert_future = insert_js_result.remote(subdomain_id, js_url)
                ray.get(insert_future)
                print(f"Inserted JS URL into DB: {js_url}")
        else:
            print(f"Error executing Katana command for URL: {url}")

    def process_task(self, task):
        url = task.get('url')
        user_id = task.get('user_id')
        root_domain = task.get('root_domain')
        
        self.increment_task_count(root_domain, user_id)

        if not url or not user_id or not root_domain:
            print(f"Invalid task received: {task}")
            return

        start_message = f"[Crawl] Starting crawling for URL: {url}"
        admin_start_message = f"{start_message} - User ID: {user_id}"
        self.send_slack_notification(user_id, start_message)
        self.send_admin_slack_notification(admin_start_message)

        try:
            parsed_url = urlparse(url)
            subdomain_id_query = "SELECT subdomain_id FROM subdomains WHERE subdomain = %s"
            subdomain_id = execute_db_query(subdomain_id_query, (parsed_url.netloc,), fetch_one=True)
            if not subdomain_id:
                print(f"Subdomain ID not found for URL: {url}")
                return

            self.process_url(subdomain_id, url)

        except Exception as e:
            print(f"Error processing URL {url}: {e}")

        finally:
            if self.decrement_task_count(root_domain, user_id):
                completion_message = f"Crawl task completed for {root_domain}"
                self.push_notification_to_queue(user_id, completion_message)

    def run(self):
        try:
            while True:
                task_data = self.fetch_task()
                if task_data:
                    queue_name, task = task_data
                    if task:
                        self.process_task(task)
        except KeyboardInterrupt:
            print("Shutting down JavaScriptGatheringWorker gracefully...")

if __name__ == "__main__":
    ray.init()

    num_workers = 5
    crawl_workers = [JavaScriptGatheringWorker.remote(queue_names=[f'crawl_queue_{i}']) for i in range(num_workers)]

    for worker in crawl_workers:
        worker.run.remote()

    try:
        print("Crawl Workers have been started. Main script will now wait indefinitely.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down Crawl Workers gracefully...")