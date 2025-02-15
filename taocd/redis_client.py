import redis
import os
import time
from taocd.utils.logger import logger
from dotenv import load_dotenv
load_dotenv()

REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 3))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', 'your_redis_password')
print(f"REDIS_HOST: {REDIS_HOST}")
class RedisClient:
    def __init__(self):
        self.client = self._create_client()

    def __str__(self):
        return f"redis_client"

    def _create_client(self):
        return redis.StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD
        )

    def get_client(self):
        return self.client

    def execute_with_reconnect(self, func, *args, **kwargs):
        attempt = 0
        while attempt < 5:
            try:
                return func(*args, **kwargs)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                attempt += 1
                logger.info(f"[taocd.redis_client] Connection error: {e}. Attempting to reconnect... (Attempt {attempt})")
                self.client = self._create_client()

    def safe_get(self, key):
        return self.execute_with_reconnect(self.client.get, key)
    
    def safe_lrange(self, key, start, end):
        return self.execute_with_reconnect(self.client.lrange, key, start, end)

    def safe_set(self, key, value):
        return self.execute_with_reconnect(self.client.set, key, value)
    
    def safe_ping(self):
        return self.execute_with_reconnect(self.client.ping)

# 使用 RedisClient
redis_client = RedisClient()

try:
    redis_client.safe_ping()
    logger.info(f"[taocd.redis_client] Connected to Redis")
except redis.ConnectionError:
    logger.info(f"[taocd.redis_client] Could not connect to Redis")
