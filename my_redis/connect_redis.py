import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from time import monotonic

class RedisManager:
    """Enhanced Redis connection manager with smart reconnection logic."""
    redis_url = AppConfig.redis_url
    _instance = None
    _last_successful_ping = 0
    _connection_attempts = 0
    MAX_RETRY_INTERVAL = 30  # Maximum seconds between retries
    HEALTH_CHECK_INTERVAL = 60  # Seconds between proactive health checks

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.redis_client = None
            cls._instance._connection_lock = asyncio.Lock()
            cls._instance._connection_state = "disconnected"
        return cls._instance

    async def init_redis(self) -> bool:
        """Initialize Redis connection with thread-safe locking."""
        async with self._connection_lock:
            if self._connection_state == "connected" and await self._quick_ping():
                return True

            if self.redis_client and await self._quick_ping():
                return True

            try:
                logger.info("🔃 Establishing Redis connection...")
                self.redis_client = await redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_keepalive=True,
                    socket_timeout=5,
                    max_connections=AppConfig.redis_max_connections,
                    health_check_interval=30,
                    retry_on_timeout=True,
                    socket_connect_timeout=5,
                )

                if await self._verified_ping():
                    self._connection_attempts = 0
                    self._last_successful_ping = monotonic()
                    logger.success("✅ Redis connection established")
                    self._connection_state = "connected"
                    return True

                await self._cleanup_failed_connection()
                self._connection_state = "disconnected"
                return False

            except Exception as e:
                logger.error(f"❌ Redis connection failed: {str(e)}")
                await self._cleanup_failed_connection()
                self._connection_state = "disconnected"
                return False

    async def check_redis_connection(self) -> bool:
        """Smart connection checker with exponential backoff."""
        # Fast path - recently verified connection
        if self._connection_state == "connected" and \
        (monotonic() - self._last_successful_ping) < self.HEALTH_CHECK_INTERVAL:
            return self.redis_client

        try:
            # Perform thorough verification
            if await self._verified_ping():
                self._last_successful_ping = monotonic()
                self._connection_attempts = 0
                return self.redis_client
        except Exception as e:
            logger.warning(f"⚠️ Redis health check failed: {str(e)}")

        # Calculate backoff with jitter
        base_delay = min(2 ** self._connection_attempts, self.MAX_RETRY_INTERVAL)
        jitter = base_delay * 0.1  # 10% jitter
        delay = base_delay + (jitter * (2 * (asyncio.get_running_loop().time() % 1) - 1))

        logger.warning(f"⏳ Reconnecting in {delay:.1f}s (attempt {self._connection_attempts + 1})")
        await asyncio.sleep(delay)

        self._connection_attempts += 1
        if await self.init_redis():
            return self.redis_client
        return None

    async def _verified_ping(self) -> bool:
        if not self.redis_client:
            return False

        try:
            # Only force reconnect if connection is stale AND ping fails
            is_stale = (monotonic() - self._last_successful_ping) > 300
            if not is_stale:
                return await asyncio.wait_for(self.redis_client.ping(), timeout=2)

            # Only proceed with cleanup if ping fails
            if not await asyncio.wait_for(self.redis_client.ping(), timeout=2):
                await self._cleanup_failed_connection()
                return False
            return True

        except Exception:
            await self._cleanup_failed_connection()
            return False

    async def _quick_ping(self) -> bool:
        """Fast path ping check without error handling."""
        try:
            return await self.redis_client.ping()
        except Exception:
            return False

    @asynccontextmanager
    async def get_connection(self):
        """Provides a Redis connection from the pool as a context manager.

        The connection is automatically released when the context is exited.
        """
        client = await self.check_redis_connection()
        if not client:
            raise ConnectionError("Redis connection not available")
        async with client.client() as conn:
            try:
                yield conn
            finally:
                # The context manager from client.client() handles the release
                pass

    async def _cleanup_failed_connection(self):
        """Safely clean up after failed connection attempts."""
        if self.redis_client:
            try:
                await self.redis_client.close()
            except Exception:
                pass
            finally:
                self.redis_client = None

    async def close_redis(self):
        """Gracefully close the Redis connection."""
        async with self._connection_lock:
            if self.redis_client:
                try:
                    await self.redis_client.close()
                    logger.success("✅ Redis connection closed cleanly")
                except Exception as e:
                    logger.error(f"⚠️ Error closing Redis connection: {str(e)}")
                finally:
                    self.redis_client = None
                    self._last_successful_ping = 0
