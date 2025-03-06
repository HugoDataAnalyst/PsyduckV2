import redis
import config as AppConfig
from utils.logger import logger

# Connect to Redis
try:
    logger.info("Connecting to Redis...")
    redis_client = redis.Redis.from_url(AppConfig.redis_url, decode_responses=True)
    if redis_client:
        logger.success("Connected to Redis!")
        redis_client.ping() # Test connection
    else:
        logger.error("Failed to connect to Redis!")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_client = None  # Prevent crashes if Redis is unavailable
