import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger
import asyncio
import os
from typing import Optional
from contextlib import asynccontextmanager
from time import monotonic

class RedisManager:
    """Enhanced Redis connection manager with smart reconnection logic and multi-worker support."""
    redis_url = AppConfig.redis_url
    _instance = None
    _last_successful_ping = 0
    _connection_attempts = 0
    MAX_RETRY_INTERVAL = 30  # Maximum seconds between retries
    HEALTH_CHECK_INTERVAL = 15  # Seconds between proactive health checks (reduced for sustained load)
    _reconnect_in_progress = False  # Prevent thundering herd

    @classmethod
    def _get_per_worker_max_connections(cls) -> int:
        """
        Calculate max connections per worker to prevent exhausting Redis.
        Divides total configured connections by number of workers, with a minimum floor.
        """
        total_max = AppConfig.redis_max_connections
        workers = max(1, AppConfig.uvicorn_workers)
        # Each worker gets a share of connections, with a minimum of 10
        per_worker = max(10, total_max // workers)
        return per_worker

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.redis_client = None
            cls._instance._connection_lock = None  # Lazily created to avoid semaphore leaks
            cls._instance._connection_state = "disconnected"
        return cls._instance

    def _get_connection_lock(self) -> asyncio.Lock:
        """Lazily create the connection lock in the worker's event loop."""
        if self._connection_lock is None:
            self._connection_lock = asyncio.Lock()
        return self._connection_lock

    async def init_redis(self) -> bool:
        """Initialize Redis connection with thread-safe locking."""
        lock = self._get_connection_lock()
        async with lock:
            if self._connection_state == "connected" and await self._quick_ping():
                return True

            if self.redis_client and await self._quick_ping():
                return True

            try:
                per_worker_max = self._get_per_worker_max_connections()
                logger.info(f"🔃 Establishing Redis connection (max_connections={per_worker_max} for this worker)...")
                self.redis_client = await redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_keepalive=True,
                    socket_timeout=5,
                    max_connections=per_worker_max,
                    health_check_interval=30,
                    retry_on_timeout=True,
                    socket_connect_timeout=5,
                )

                if await self._verified_ping():
                    self._connection_attempts = 0
                    self._last_successful_ping = monotonic()
                    logger.success(f"✅ Redis connection established (pool size: {per_worker_max})")
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

    async def check_redis_connection(self):
        """
        Smart connection checker with exponential backoff and circuit breaker.

        Circuit breaker prevents thundering herd: when many concurrent requests
        find the connection down, only ONE will attempt reconnection while
        others wait for the result.
        """
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

        # Circuit breaker: If reconnection is already in progress, wait briefly and return
        # This prevents thundering herd where many concurrent requests all try to reconnect
        if self._reconnect_in_progress:
            # Wait a bit for the ongoing reconnection to complete
            for _ in range(10):  # Wait up to 5 seconds
                await asyncio.sleep(0.5)
                if self._connection_state == "connected" and self.redis_client:
                    return self.redis_client
                if not self._reconnect_in_progress:
                    break
            # Return current state (may still be None)
            return self.redis_client if self._connection_state == "connected" else None

        # Mark reconnection in progress (circuit breaker closed)
        self._reconnect_in_progress = True

        try:
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
        finally:
            # Open circuit breaker
            self._reconnect_in_progress = False

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

    async def get_connection_with_retry(self, max_attempts: int = 3, delay: float = 0.5):
        """
        Get Redis connection with fast retries for critical operations like webhook processing.

        Unlike check_redis_connection() which uses exponential backoff for background tasks,
        this method uses short delays suitable for real-time webhook processing where we
        can't afford to wait long but also don't want to lose data.

        Args:
            max_attempts: Number of retry attempts (default 3)
            delay: Delay between attempts in seconds (default 0.5s)

        Returns:
            Redis client if connected, None if all attempts fail
        """
        for attempt in range(1, max_attempts + 1):
            # Fast path - recently verified connection
            if self._connection_state == "connected" and \
               (monotonic() - self._last_successful_ping) < self.HEALTH_CHECK_INTERVAL:
                return self.redis_client

            # Try to get/verify connection
            try:
                if self.redis_client and await self._quick_ping():
                    self._last_successful_ping = monotonic()
                    return self.redis_client
            except Exception:
                pass

            # Need to reconnect - but don't use full backoff for webhook processing
            if attempt < max_attempts:
                # Quick retry without the full reconnection ceremony
                if not self._reconnect_in_progress:
                    try:
                        self._reconnect_in_progress = True
                        if await self.init_redis():
                            return self.redis_client
                    finally:
                        self._reconnect_in_progress = False

                await asyncio.sleep(delay)

        # All attempts failed - try one more time with full reconnection
        return await self.check_redis_connection()

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
        """Gracefully close the Redis connection and clean up resources."""
        lock = self._get_connection_lock()
        async with lock:
            if self.redis_client:
                try:
                    await self.redis_client.close()
                    logger.success("✅ Redis connection closed cleanly")
                except Exception as e:
                    logger.error(f"⚠️ Error closing Redis connection: {str(e)}")
                finally:
                    self.redis_client = None
                    self._last_successful_ping = 0
                    self._connection_state = "disconnected"

        # Clean up the lock to help prevent semaphore leaks
        self._connection_lock = None
