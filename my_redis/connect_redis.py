import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger

class RedisManager:
    """Handles Redis connection, reconnection, and closure."""
    # Set class level attributes
    redis_url = AppConfig.redis_url

    def __init__(self):
        """Initialiaze Redis client as None."""
        self.redis_client = None

    # Connect to Redis
    async def init_redis(self):
        """Initialize Redis connection and handle errors."""
        try:
            logger.info("🔃 Connecting to Redis...")
            self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)
            if await self.redis_client.ping(): # test connection
                logger.success("✅ Connected to Redis!")
                return True
            else:
                logger.error("❌ Failed to connect to Redis!")
                self.redis_client = None

        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            self.redis_client = None  # Prevent crashes if Redis is unavailable
        return None

    async def close_redis(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            logger.success("✅ Closed Redis connection.")
            self.redis_client = None
        else:
            logger.warning("⚠️ Redis connection is already closed.")


    async def check_redis_connection(self):
        """Check if Redis connection is running. This is an utilitary function."""
        if not self.redis_client:
            logger.warning("⚠️ Redis connection lost. 🔃 Attempting to reconnect...")
            result = await self.init_redis()
            if result is None:
                return False

        if self.redis_client is not None:
            logger.debug("✅ Redis connection is active.")
            return True
