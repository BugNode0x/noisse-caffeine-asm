import redis
import json
import requests
import time
import hashlib
from config import REDIS_HOST, REDIS_PORT, REDIS_PWD, ADMIN_WEBHOOK
from .db_processor import get_user_webhook

class BaseWorker:
    def __init__(self, queue_names=None):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, decode_responses=True)
        self.queue_names = queue_names if queue_names is not None else ['api_queue']

    def send_slack_notification(self, user_id, message):
        webhook_url = get_user_webhook(user_id)
        if not webhook_url:
            print(f"No webhook URL configured for user ID: {user_id}")
            return
        
        slack_data = {'text': message}
        response = requests.post(webhook_url, json=slack_data, headers={'Content-Type': 'application/json'})

        if response.status_code != 200:
            print(f"Slack notification failed for user ID {user_id}: {response.status_code} - {response.text}")

    def send_admin_slack_notification(self, message):
        admin_webhook_url = ADMIN_WEBHOOK
        slack_data = {'text': message}
        response = requests.post(admin_webhook_url, json=slack_data, headers={'Content-Type': 'application/json'})

        if response.status_code != 200:
            print(f"Admin Slack notification failed: {response.status_code} - {response.text}")
    
    def perform_health_check(self):
        # Placeholder for health check logic
        # Return True if healthy, False if not
        return True  # For now, we just assume it's always healthy

    def fetch_task(self):
        for queue_name in self.queue_names:
            task_data = self.redis_client.blpop(queue_name, timeout=1)
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

    def increment_task_count(self, root_domain, user_id):
        redis_key = f"task_count:{root_domain}:{user_id}"
        self.redis_client.incr(redis_key)
        self.redis_client.expire(redis_key, 4000)

    def decrement_task_count(self, root_domain, user_id):
        redis_key = f"task_count:{root_domain}:{user_id}"
        remaining_tasks = self.redis_client.decr(redis_key)
        return remaining_tasks <= 0

    def select_queue_index(self, identifier):
        hash_value = int(hashlib.md5(identifier.encode()).hexdigest(), 16)
        num_queues = 10
        return hash_value % num_queues

    def push_notification_to_queue(self, user_id, message):
        notification_task = json.dumps({'user_id': user_id, 'message': message})
        self.redis_client.rpush('notification_queue', notification_task)

    def run(self):
        last_health_check_time = time.time()
        health_check_interval = 60  # seconds

        while True:
            current_time = time.time()
            if current_time - last_health_check_time > health_check_interval:
                if not self.perform_health_check():
                    self.send_admin_slack_notification(f"Health check failed for {type(self).__name__}")
                last_health_check_time = current_time

            task_data = self.fetch_task()
            if task_data:
                queue_name, task = task_data
                if task:
                    try:
                        self.process_task(task)
                    except Exception as e:
                        crash_message = f"Worker {type(self).__name__} crashed with error: {str(e)}"
                        self.send_admin_slack_notification(crash_message)
                        raise