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
from my_redis.utils import maintenance
from my_redis.utils.mysql_backup import (
    MySQLBackup, MySQLCleanup,
    check_backup_counts, restore_counters, restore_timeseries,
    redis_run_id, redis_uptime_s, get_backup_run_id, set_backup_run_id, first_data_key,
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

                # Never alongside the Redis cleanup: both walk the whole keyspace
                async with maintenance.exclusive("Redis backup"):
                    t0 = time.perf_counter()
                    if not await self._reconcile(client, "backup cycle"):
                        continue
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

    @staticmethod
    async def _reconcile(client, context: str) -> bool:
        """
        Before writing a backup, make sure Redis has not restarted underneath
        us: backing up a freshly emptied Redis would overwrite good rows with
        partial data. Returns False (skip this backup) if that cannot be checked.
        """
        try:
            await reconcile_with_backup(client, context)
            return True
        except Exception as e:
            logger.error("❌ Redis backup ({}): could not check Redis against the backup ({}) "
                         "- skipping so existing backup rows are not overwritten", context, e)
            return False

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

        # Final backup before shutdown. Deliberately not behind
        # maintenance.exclusive(): waiting out a cleanup pass here could
        # outlast the shutdown grace period and lose the backup entirely.
        try:
            client = await self._redis_manager.check_redis_connection()
            if client and await self._reconcile(client, "final backup"):
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


_reconcile_lock = asyncio.Lock()


async def reconcile_with_backup(client, context: str) -> str:
    """
    Bring Redis and the MySQL backup back in step, without ever overwriting
    live data. Decided by which Redis process (run_id) the backup came from:

      same run_id        Redis still holds everything the backup has (an app
                         restart or crash) -> leave Redis alone. Restoring here
                         is what rewound live counters and resurrected keys
                         that cleanup had already deleted.
      different run_id   Redis restarted and lost its data -> merge the backup
                         in: missing keys restored, live keys incremented.
      no run_id recorded first run of this version. An empty Redis is restored
                         in full. A Redis that already holds data only gets
                         back the keys it lost when it last started (backed up
                         before that, absent now); keys it holds are left alone,
                         since the app that ran before may already have
                         restored them.

    Only rows backed up before the current Redis process started are merged:
    newer rows were taken from this same Redis, so their counts are already live.

    Serialised: two concurrent calls could both see the old run_id and merge
    the same backup twice.

    Returns a short label for logs and tests.
    """
    async with _reconcile_lock:
        return await _reconcile_unlocked(client, context)


async def _reconcile_unlocked(client, context: str) -> str:
    current = await redis_run_id(client)
    recorded = await get_backup_run_id()

    if current and recorded == current:
        logger.info("Redis restore ({}): Redis still holds the data the backup came from "
                    "(run_id {}) - nothing to restore", context, current[:8])
        return "skip:same-redis"

    uptime = await redis_uptime_s(client)
    only_absent = False
    if recorded is None:
        sample = await first_data_key(client)
        if sample:
            only_absent = True
            logger.warning("Redis restore ({}): no backup provenance yet and Redis already holds data "
                           "(e.g. {}) - restoring only keys backed up before Redis started {}s ago "
                           "and missing now; live keys are left alone", context, sample, uptime)
        else:
            uptime = None   # empty Redis: every backed-up row is missing
            logger.warning("Redis restore ({}): Redis holds no data - restoring the MySQL backup", context)
    else:
        logger.warning("Redis restore ({}): Redis restarted since the last backup (run_id {} -> {}, up {}s) "
                       "- merging the MySQL backup into Redis", context,
                       recorded[:8], current[:8] if current else "?", uptime)

    # Record first: if the merge is interrupted, the next attempt must not add
    # the same counts a second time.
    if current:
        await set_backup_run_id(current)
    t0 = time.perf_counter()
    await restore_counters(client, older_than_s=uptime, only_absent=only_absent)
    await restore_timeseries(client, older_than_s=uptime, only_absent=only_absent)
    logger.success("✅ Redis restore complete in {:.2f}s", time.perf_counter() - t0)
    return "recovered-missing" if only_absent else "restored"


async def reconcile_now(redis_manager: RedisManager, context: str) -> None:
    """Fire-and-forget reconcile (e.g. right after a Redis restart is noticed)."""
    if not AppConfig.redis_mysql_backups:
        return
    try:
        client = await redis_manager.check_redis_connection()
        if client:
            await reconcile_with_backup(client, context)
    except Exception as e:
        logger.error("Redis restore ({}): reconciliation failed: {} - the next backup cycle retries", context, e)


class RedisRestoreService:
    """
    One-shot MySQL → Redis reconciliation, called once at leader startup before
    services begin. Only active when REDIS_MYSQL_BACKUPS = true.

      MySQL unreachable      skip (warn)
      backup tables empty    skip (fresh start)
      otherwise              reconcile_with_backup(): restore only when Redis
                             actually lost its data
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

        client = await self._redis_manager.check_redis_connection()
        if not client:
            logger.error("Redis restore: Redis connection unavailable — skipping restore")
            return

        logger.info("Redis restore: backup holds {} counter rows, {} timeseries rows", counter_rows, ts_rows)
        try:
            await reconcile_with_backup(client, "startup")
        except Exception as e:
            logger.error("Redis restore: reconciliation failed: {}", e)
