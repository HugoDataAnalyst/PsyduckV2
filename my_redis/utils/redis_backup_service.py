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
    redis_dataset_id, mint_dataset_id, redis_uptime_s, get_backup_dataset_id, first_data_key,
    get_meta, set_meta, META_LAST_BACKUP, META_LAST_FULL,
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
                    kind = await self._backup_once(client)
                logger.success(
                    "✅ Backup cycle ({}) complete in {:.2f}s — rescheduling in {}s",
                    kind, time.perf_counter() - t0, self._interval,
                )

            except asyncio.CancelledError:
                logger.info("🛑 Redis backup loop cancelled")
                break
            except Exception as e:
                logger.error("❌ Redis backup cycle failed: {}", e)

    async def _backup_window(self) -> float | None:
        """
        Start of the previous successful backup, to back up only what changed
        since - or None for a full backup: on the first run, after a restore or
        merge (which clears the last-full mark), and every full_backup_interval_hours.
        """
        full_every = AppConfig.redis_full_backup_interval_hours * 3600
        last = await get_meta(META_LAST_BACKUP)
        last_full = await get_meta(META_LAST_FULL)
        if full_every <= 0 or last is None or last_full is None:
            return None
        if time.time() - float(last_full) >= full_every:
            return None
        return float(last)

    async def _backup_once(self, client) -> str:
        """Cleanup + backup of counters and timeseries. Returns "full" or "incremental"."""
        since = await self._backup_window()
        started = time.time()
        await _run_with_retry("Cleanup counter backup",    MySQLCleanup.counters)
        await _run_with_retry("Cleanup timeseries backup", MySQLCleanup.timeseries, self._interval)
        ok_counters = await _run_with_retry("Backup counters", MySQLBackup.counters, client, since_utc=since)
        ok_ts = await _run_with_retry("Backup timeseries", MySQLBackup.timeseries, client, since_utc=since)
        # Only a complete backup moves the window forward; after a failure the
        # next cycle starts from the last good one again.
        if ok_counters and ok_ts:
            await set_meta(META_LAST_BACKUP, str(started))
            if since is None:
                await set_meta(META_LAST_FULL, str(started))
        return "full" if since is None else "incremental"

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
                kind = await self._backup_once(client)
                logger.success("✅ Final Redis backup ({}) complete in {:.2f}s", kind, time.perf_counter() - t0)
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
    live data. Decided by the dataset id stored in Redis (DATASET_KEY) against
    the one recorded with the backup:

      same id            Redis still holds the data the backup came from - an
                         app restart or crash, or a Redis restart that reloaded
                         its data -> leave Redis alone. Restoring here is what
                         rewound live counters and resurrected cleaned keys.
      other id           Redis holds a different dataset (an older RDB restored
                         by hand): it is what the operator chose, so adopt it -
                         merging would add the backup's counts on top of it.
      no id, empty Redis Redis lost everything -> restore the backup in full.
      no id, data        Redis lost its data and has refilled since (Redis
                         restarted under the running app) -> merge: missing
                         keys restored, live keys incremented. Only rows backed
                         up before this Redis started are used; newer rows were
                         taken from this same Redis and are already live.
                         First run of this version (nothing recorded yet): the
                         app before it may already have restored keys Redis
                         holds, so only missing keys are restored.

    Serialised: two concurrent calls could both find no id and merge the same
    backup twice.

    Returns a short label for logs and tests.
    """
    async with _reconcile_lock:
        return await _reconcile_unlocked(client, context)


async def _reconcile_unlocked(client, context: str) -> str:
    current = await redis_dataset_id(client)
    recorded = await get_backup_dataset_id()

    if current and current == recorded:
        logger.info("Redis restore ({}): Redis still holds the data the backup came from "
                    "(dataset {}) - nothing to restore", context, current[:8])
        return "skip:same-dataset"

    if current:
        logger.warning("Redis restore ({}): Redis holds dataset {} but the backup is from {} "
                       "(an RDB restored by hand?) - adopting the Redis data, not merging",
                       context, current[:8], recorded[:8] if recorded else "none")
        await mint_dataset_id(client, current)
        return "skip:adopted"

    uptime = await redis_uptime_s(client)
    only_absent = False
    sample = await first_data_key(client)
    if not sample:
        uptime = None   # empty Redis: every backed-up row is missing
        logger.warning("Redis restore ({}): Redis holds no data - restoring the MySQL backup", context)
    elif recorded is None:
        only_absent = True
        logger.warning("Redis restore ({}): no backup provenance yet and Redis already holds data "
                       "(e.g. {}) - restoring only keys backed up before Redis started {}s ago "
                       "and missing now; live keys are left alone", context, sample, uptime)
    else:
        logger.warning("Redis restore ({}): Redis lost the data the backup came from (dataset {}) "
                       "and refilled since it started {}s ago - merging the MySQL backup into Redis",
                       context, recorded[:8], uptime)

    # Tag first: if the merge is interrupted, the next attempt must not add
    # the same counts a second time.
    await mint_dataset_id(client)
    # The merge can touch keys of any age: the next backup must be a full one
    await set_meta(META_LAST_FULL, None)
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
