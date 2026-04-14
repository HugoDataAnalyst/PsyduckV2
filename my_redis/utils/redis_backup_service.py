"""
Periodic Redis → MySQL backup service.

Runs on the configured interval (default 3600s / 1 hour) and also
performs a final backup on graceful shutdown. Only active when
REDIS_MYSQL_BACKUPS = true and the worker is the leader.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import config as AppConfig
from my_redis.connect_redis import RedisManager
from my_redis.utils.mysql_backup import (
    MySQLBackup, MySQLCleanup,
    check_backup_counts, restore_counters, restore_timeseries,
)
from utils.logger import logger

_INITIAL_RETRY_DELAY = 5   # seconds
_MAX_RETRY_DELAY     = 60  # seconds (exponential cap)
_MAX_ATTEMPTS        = 5


async def _run_with_retry(label: str, coro_factory, *args, **kwargs) -> bool:
    """
    Run an async job with exponential backoff.

    coro_factory is called fresh on each attempt so exhausted coroutines
    are never re-awaited. Returns True on success, False after all attempts
    are exhausted (caller continues — jobs are independent).
    """
    delay = _INITIAL_RETRY_DELAY
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            await coro_factory(*args, **kwargs)
            logger.success("✅ {} complete", label)
            return True
        except Exception as e:
            if attempt < _MAX_ATTEMPTS:
                logger.warning(
                    "⚠️ {} failed (attempt {}/{}): {} — retrying in {}s",
                    label, attempt, _MAX_ATTEMPTS, e, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, _MAX_RETRY_DELAY)
            else:
                logger.error(
                    "❌ {} failed after {} attempts: {} — will retry at next scheduled interval",
                    label, _MAX_ATTEMPTS, e,
                )
    return False


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
                next_run = datetime.now(timezone.utc) + timedelta(seconds=self._interval)
                logger.info(
                    "⏳ Next backup cycle scheduled at {} UTC (~{}s)",
                    next_run.strftime("%Y-%m-%d %H:%M:%S"), self._interval,
                )
                await asyncio.sleep(self._interval)
                if not self._running:
                    break

                client = await self._redis_manager.check_redis_connection()
                if not client:
                    logger.warning("⚠️ Redis backup: connection unavailable, skipping cycle")
                    continue

                t0 = time.perf_counter()
                await _run_with_retry("Cleanup counter backup",    MySQLCleanup.counters)
                await _run_with_retry("Cleanup timeseries backup", MySQLCleanup.timeseries, self._interval)
                await _run_with_retry("Backup counters",           MySQLBackup.counters,    client)
                await _run_with_retry("Backup timeseries",         MySQLBackup.timeseries,  client)
                logger.success(
                    "✅ Backup cycle complete in {:.2f}s — rescheduling in {}s",
                    time.perf_counter() - t0, self._interval,
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
                await _run_with_retry("Cleanup counter backup",    MySQLCleanup.counters)
                await _run_with_retry("Cleanup timeseries backup", MySQLCleanup.timeseries, self._interval)
                await _run_with_retry("Backup counters",           MySQLBackup.counters,    client)
                await _run_with_retry("Backup timeseries",         MySQLBackup.timeseries,  client)
                logger.success("✅ Final Redis backup complete in {:.2f}s", time.perf_counter() - t0)
        except Exception as e:
            logger.error("❌ Final Redis backup failed: {}", e)

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


class RedisRestoreService:
    """
    One-shot MySQL → Redis restore, called once at leader startup before services begin.
    Only active when REDIS_MYSQL_BACKUPS = true.

    Three outcomes from check_backup_counts():
      None    → MySQL unreachable          — skip restore, warn
      (0, 0)  → backup tables empty        — skip restore (fresh start)
      (n, m)  → data present               — restore counters + timeseries
    """

    def __init__(self, redis_manager: RedisManager):
        self._redis_manager = redis_manager

    async def restore(self) -> None:
        if not AppConfig.redis_mysql_backups:
            logger.info("Redis restore: REDIS_MYSQL_BACKUPS disabled — skipping")
            return

        counts = await check_backup_counts()
        if counts is None:
            logger.warning("Redis restore: MySQL unreachable — skipping restore")
            return

        counter_rows, ts_rows = counts
        if counter_rows == 0 and ts_rows == 0:
            logger.info("Redis restore: backup tables empty — no restore needed (fresh start)")
            return

        logger.info(
            "Redis restore: starting — {} counter rows, {} timeseries rows",
            counter_rows, ts_rows,
        )

        client = await self._redis_manager.check_redis_connection()
        if not client:
            logger.error("Redis restore: Redis connection unavailable — skipping restore")
            return

        t0 = time.perf_counter()
        await restore_counters(client)
        await restore_timeseries(client)
        logger.success("✅ Redis restore complete in {:.2f}s", time.perf_counter() - t0)
