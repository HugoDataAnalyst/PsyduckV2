"""
Periodic Redis → MySQL backup service.

Runs on the configured interval (default 3600s / 1 hour) and also
performs a final backup on graceful shutdown. Only active when
REDIS_MYSQL_BACKUPS = true and the worker is the leader.
"""

import asyncio
import time

from my_redis.connect_redis import RedisManager
from my_redis.utils.mysql_backup import backup_all, cleanup_mysql_backup
from utils.logger import logger

_INITIAL_RETRY_DELAY = 5   # seconds
_MAX_RETRY_DELAY     = 60  # seconds (exponential cap)
_MAX_ATTEMPTS        = 5


class RedisBackupService:
    def __init__(self, redis_manager: RedisManager, interval: int = 3600):
        self._redis_manager = redis_manager
        self._interval      = interval
        self._running       = False
        self._task: asyncio.Task | None = None

    async def _backup_loop(self) -> None:
        self._running = True
        logger.info("⏳ Redis backup service started — interval: {}s", self._interval)

        while self._running:
            try:
                await asyncio.sleep(self._interval)
                if not self._running:
                    break

                client = await self._redis_manager.check_redis_connection()
                if not client:
                    logger.warning("⚠️ Redis backup: connection unavailable, skipping cycle")
                    continue

                t0 = time.perf_counter()
                delay = _INITIAL_RETRY_DELAY
                for attempt in range(1, _MAX_ATTEMPTS + 1):
                    try:
                        await cleanup_mysql_backup(self._interval)
                        await backup_all(client)
                        logger.success(
                            "✅ Redis backup cycle complete in {:.2f}s", time.perf_counter() - t0
                        )
                        break
                    except Exception as e:
                        if attempt < _MAX_ATTEMPTS:
                            logger.warning(
                                "⚠️ Backup cycle failed (attempt {}/{}): {} — retrying in {}s",
                                attempt, _MAX_ATTEMPTS, e, delay,
                            )
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, _MAX_RETRY_DELAY)
                        else:
                            logger.error(
                                "❌ Backup cycle failed after {} attempts: {} — will retry at next scheduled interval",
                                _MAX_ATTEMPTS, e,
                            )

            except asyncio.CancelledError:
                logger.info("🛑 Redis backup loop cancelled")
                break
            except Exception as e:
                logger.error("❌ Redis backup cycle failed: {}", e)

    async def start(self) -> None:
        if self._running:
            logger.warning("⚠️ Redis backup service already running")
            return
        self._task = asyncio.create_task(self._backup_loop())
        logger.info("🚀 Started Redis backup service")

    async def stop(self) -> None:
        if not self._running:
            logger.warning("⚠️ Redis backup service already stopped")
            return

        self._running = False

        # Final backup before shutdown
        try:
            client = await self._redis_manager.check_redis_connection()
            if client:
                t0 = time.perf_counter()
                logger.info("🔚 Final Redis backup on shutdown...")
                await cleanup_mysql_backup(self._interval)
                await backup_all(client)
                logger.success(
                    "✅ Final Redis backup complete in {:.2f}s", time.perf_counter() - t0
                )
        except Exception as e:
            logger.error("❌ Final Redis backup failed: {}", e)

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
