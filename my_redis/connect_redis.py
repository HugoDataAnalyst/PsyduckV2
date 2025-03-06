import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger

redis_client = None  # Global variable to store Redis connection

# Connect to Redis
async def init_redis():
    """Initialize Redis connection and handle errors."""
    global redis_client
    try:
        logger.info("Connecting to Redis...")
        redis_client = await redis.from_url(AppConfig.redis_url, decode_responses=True)
        if await redis_client.ping(): # test connection
            logger.success("Connected to Redis!")
        else:
            logger.error("Failed to connect to Redis!")
            redis_client = None
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        redis_client = None  # Prevent crashes if Redis is unavailable
