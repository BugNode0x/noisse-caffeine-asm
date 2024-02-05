import redis
import json
from config import REDIS_HOST, REDIS_PORT, REDIS_PWD

class BaseWorker:
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, decode_responses=True)

    def fetch_task(self):
        task_data = self.redis_client.blpop('api_queue', 30)
        if task_data:
            queue_name, task_data_str = task_data
            print(f"Fetched task from {queue_name}: {task_data_str}")
            try:
                return json.loads(task_data_str)
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e} for task data: {task_data_str}")
        return None

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
