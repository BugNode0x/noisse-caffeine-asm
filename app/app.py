import sys
import redis
import json  # Import the json module
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(parent_dir))
from flask import Flask, request, jsonify
from config import REDIS_HOST, REDIS_PORT, REDIS_PWD

# Create Redis client 
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, decode_responses=True)

app = Flask(__name__)

@app.route('/monitor-domain', methods=['POST'])
def monitor_domain():
    user_id = request.headers.get('X-User-ID')
    token = request.headers.get('Authorization')
    data = request.json

    # Validate the incoming data
    if not user_id or not token or not data or 'domain' not in data:
        return jsonify({"error": "Invalid request"}), 400

    # Enqueue the task in Redis
    task = {"domain": data['domain'], "user_id": user_id}
    redis_client.rpush('api_queue', json.dumps(task))

    return jsonify({"message": "Task enqueued successfully"})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
