import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger

class RedisManager:
    """Handles Redis connection, reconnection, and closure."""
    # Set class level attributes
    redis_url = AppConfig.redis_url
    _instance = None # ✅ Singleton instance

    def __new__(cls):
        """Ensures only one RedisManager instance is created (Singleton)."""
        if cls._instance is None:
            cls._instance = super(RedisManager, cls).__new__(cls)
            cls._instance.redis_client = None  # Initialize connection as None
        return cls._instance

    # Connect to Redis
    async def init_redis(self):
        """Initialize Redis connection and handle errors."""
        if self.redis_client:  # ✅ Prevent reinitializing an active connection
            return True

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
        return False

    async def close_redis(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            logger.success("✅ Closed Redis connection.")
            self.redis_client = None
        else:
            logger.warning("⚠️ Redis connection is already closed.")


    async def check_redis_connection(self):
        """Check if Redis connection is running and attempt reconnection if necessary."""
        try:
            ping = await self.redis_client.ping()  # ✅ Await Redis ping check
            if ping:
                logger.debug("✅ Redis connection is active.")
                return True
        except Exception as e:
            logger.warning(f"⚠️ Redis connection lost: {e}. 🔃 Attempting to reconnect...")

        # ❌ If ping fails, reconnect
        result = await self.init_redis()
        if result:
            logger.success("✅ Redis connection restored.")
            return True
        else:
            return False

