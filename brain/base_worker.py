import redis
import json
import requests
from config import REDIS_HOST, REDIS_PORT, REDIS_PWD
from .db_processor import get_user_webhook

class BaseWorker:
    def __init__(self, queue_names=None):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, decode_responses=True)
        self.queue_names = queue_names if queue_names is not None else ['api_queue']

    def send_slack_notification(self, user_id, message):
        webhook_url = get_user_webhook(user_id)
        if not webhook_url:
            print("No webhook URL configured for this user.")
            return
        
        slack_data = {'text': message}

        response = requests.post(
            webhook_url, json=slack_data,
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code != 200:
            print(f"Slack notification failed: {response.status_code} - {response.text}")

    def fetch_task(self):
        for queue_name in self.queue_names:
            task_data = self.redis_client.blpop(queue_name, timeout=1)  # Set a timeout to cycle through queues
            if task_data:
                _, task_data_str = task_data
                print(f"Fetched task from {queue_name}: {task_data_str}")
                try:
                    return queue_name, json.loads(task_data_str)
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e} for task data: {task_data_str}")
        return None, None

    def process_task(self, task):
        # This method should be overridden by subclasses
        raise NotImplementedError("This method should be overridden by subclasses")

    def run(self):
        while True:
            task = self.fetch_task()
            if task:
                try:
                    self.process_task(task)  # Call process_task with the whole task dictionary
                except Exception as e:
                    print(f"Error processing task: {e}")
