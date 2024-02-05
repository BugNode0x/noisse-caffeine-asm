import redis
from config import REDIS_HOST, REDIS_PORT, REDIS_PWD

class BaseWorker:
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, decode_responses=True)


    def fetch_task(self):
        task = self.redis_client.blpop('api_queue', 30)
        return task

        
    def process_task(self, task):
        # This is a placeholder method to be overridden by specific workers
        raise NotImplementedError("This method should be overridden by subclasses")

    def run(self):
        # Main worker loop
        while True:
            task = self.fetch_task()
            if task:
                domain = task[1]  # Assuming the task contains the domain as the second item
                result = self.process_task(domain)
                # Here, you will handle the result
                # For now, we can just print it
                print(result)
