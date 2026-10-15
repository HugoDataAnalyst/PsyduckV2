"""
Redis → MySQL backup and restore utilities.

Backs up counter:* and ts:* HASH keys to MySQL so Redis can run with
persistence disabled (REDIS_MYSQL_BACKUPS = true in config.json).

Buffer keys (buffer:*) are intentionally excluded — they are transient
staging data that is always flushed to MySQL before being discarded.

Live keys (active:*) are intentionally excluded too. They are sorted sets,
not hashes, so they cannot flow through the HGETALL/HSET paths here; every
member despawns within 60 minutes, so a restore would be stale by definition;
and the sets rebuild themselves from the webhook feed within one despawn
cycle, unlike the counter/timeseries families where a lost hour is lost.

Performance:
    HGETALL calls are pipelined in chunks of _CHUNK_SIZE, so we send
    one round trip per chunk instead of one per key.  HSET restores are
    pipelined the same way.  asyncio.sleep(0) is inserted between chunks
    during periodic backups so webhook coroutines keep running.
"""

import asyncio
import uuid
import json
from datetime import datetime, timedelta, timezone
import config as AppConfig
from utils.logger import logger
from sql.connect_db import execute, executemany, fetch_val, stream_rows

# ── Key patterns to back up ───────────────────────────────────────────────────

COUNTER_PATTERNS: list[str] = ["counter:*"]

TIMESERIES_PATTERNS: list[str] = [
    "ts:pokemon:*",
    "ts:tth_pokemon:*",
    "ts:raids_total:*",
    "ts:invasion:*",
    "ts:quests_total:*",
]

_CHUNK_SIZE = 500   # keys per pipeline batch / rows per executemany batch


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _scan_keys(client, patterns: list[str]) -> list[str]:
    """SCAN Redis for all keys matching any of the given glob patterns."""
    found: list[str] = []
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor, match=pattern, count=500)
            for k in keys:
                found.append(k.decode() if isinstance(k, bytes) else k)
            if cursor == 0:
                break
    return found


def _is_counter_key_recent(key: str, now: datetime) -> bool:
    """
    Returns True if this counter key should be included in the backup.

    - Hourly keys (YYYYMMDDHH suffix): filtered per-type using AppConfig retention values.
      0 = disabled = always include.
    - Daily (YYYYMMDD) and monthly (YYYYMM) keys: always included — only ~11K total,
      no filtering needed.
    - Unrecognised format: always included (safe default).
    """
    parts = key.split(":")
    if len(parts) < 4:
        return True

    key_type = parts[1]   # e.g. "pokemon_hourly", "raid_total", "pokemon_weather_iv"
    date_str = parts[-1]

    # Hourly keys have a 10-digit YYYYMMDDHH suffix
    if len(date_str) == 10 and date_str.isdigit():
        _retention_map: dict[str, int] = {
            "pokemon_hourly":     AppConfig.counter_pokemon_hourly_retention_hours,
            "tth_pokemon_hourly": AppConfig.counter_tth_pokemon_hourly_retention_hours,
            "raid_hourly":        AppConfig.counter_raid_hourly_retention_hours,
            "invasion_hourly":    AppConfig.counter_invasion_hourly_retention_hours,
            "quest_hourly":       AppConfig.counter_quest_hourly_retention_hours,
        }
        retention_hours = _retention_map.get(key_type, 168)
        if retention_hours == 0:
            return True   # 0 = keep all for this type
        try:
            return datetime.strptime(date_str, "%Y%m%d%H") >= now - timedelta(hours=retention_hours)
        except ValueError:
            return True

    # 8-digit suffix: could be a *_daily key (apply retention) or *_total/*_weather_iv (always keep)
    if len(date_str) == 8 and date_str.isdigit():
        _daily_retention: dict[str, int] = {
            "pokemon_daily":     AppConfig.counter_pokemon_daily_retention_days,
            "tth_pokemon_daily": AppConfig.counter_tth_pokemon_daily_retention_days,
            "raid_daily":        AppConfig.counter_raid_daily_retention_days,
            "invasion_daily":    AppConfig.counter_invasion_daily_retention_days,
            "quest_daily":       AppConfig.counter_quest_daily_retention_days,
        }
        if key_type in _daily_retention:
            retention_days = _daily_retention[key_type]
            if retention_days == 0:
                return True
            try:
                return datetime.strptime(date_str, "%Y%m%d") >= now - timedelta(days=retention_days)
            except ValueError:
                return True
        # weekly totals (pokemon_total, raid_total, etc.) and weather — always keep
        return True

    # Monthly (6-digit YYYYMM) — always keep
    return True


async def _hgetall_pipeline(client, keys: list[str]) -> list[dict[str, str] | None]:
    """
    Pipeline HGETALL for a batch of keys in a single round trip.
    Returns a list parallel to `keys`; entry is None when the key is empty/missing.
    """
    pipe = client.pipeline(transaction=False)
    for key in keys:
        pipe.hgetall(key)
    results = await pipe.execute()

    decoded: list[dict[str, str] | None] = []
    for raw in results:
        if not raw:
            decoded.append(None)
            continue
        decoded.append({
            (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in raw.items()
        })
    return decoded


# ── Backup count check ────────────────────────────────────────────────────────

async def check_backup_counts() -> tuple[int, int] | None:
    """
    Returns (counter_rows, timeseries_rows) on a successful MySQL query.
    Returns None on ANY error (connection failure, missing table, etc.).

    Used at startup to decide the restore strategy:
      None     → MySQL unreachable/broken  — skip restore, do not touch MySQL
      (0, 0)   → tables empty             — seed MySQL from Redis, no restore needed
      (n, m)   → tables have data         — restore from MySQL into Redis
    """
    try:
        counter_rows = await fetch_val("SELECT COUNT(*) FROM redis_counter_backup")
        ts_rows      = await fetch_val("SELECT COUNT(*) FROM redis_timeseries_backup")
        return (int(counter_rows or 0), int(ts_rows or 0))
    except Exception as e:
        logger.error("Failed to check MySQL backup table counts: {}", e)
        return None


# ── Backup ────────────────────────────────────────────────────────────────────

class MySQLBackup:
    """Redis → MySQL backup of counter and timeseries hashes."""

    @staticmethod
    async def counters(client, *, yield_between_chunks: bool = True) -> int:
        """
        SCAN counter:* → pipelined HGETALL → upsert into redis_counter_backup.
        Returns number of keys backed up.

        yield_between_chunks: set False during startup seeding (we want to finish
        fast); leave True for periodic background backups so webhooks keep flowing.
        """
        all_keys = await _scan_keys(client, COUNTER_PATTERNS)
        if not all_keys:
            logger.debug("Redis backup: no counter keys found")
            return 0

        now  = datetime.now(timezone.utc).replace(tzinfo=None)
        keys = [k for k in all_keys if _is_counter_key_recent(k, now)]
        logger.debug(
            "Redis backup: {} counter keys total, {} within retention ({} outside window skipped)",
            len(all_keys), len(keys), len(all_keys) - len(keys),
        )

        if not keys:
            return 0

        sql = """
            INSERT INTO redis_counter_backup (redis_key, hash_data)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                hash_data    = VALUES(hash_data),
                backed_up_at = CURRENT_TIMESTAMP
        """

        backed_up = 0
        skipped   = 0

        for i in range(0, len(keys), _CHUNK_SIZE):
            chunk_keys = keys[i:i + _CHUNK_SIZE]
            results    = await _hgetall_pipeline(client, chunk_keys)

            rows: list[tuple[str, str]] = []
            for key, data in zip(chunk_keys, results):
                if data is None:
                    skipped += 1
                else:
                    rows.append((key, json.dumps(data)))

            if rows:
                await executemany(sql, rows)
                backed_up += len(rows)

            if yield_between_chunks:
                await asyncio.sleep(0)   # yield to event loop between chunks

        logger.debug(
            "Redis backup: {} counter keys saved ({} empty skipped)",
            backed_up, skipped,
        )
        return backed_up

    @staticmethod
    async def timeseries(client, *, yield_between_chunks: bool = True) -> int:
        """
        SCAN ts:* patterns → pipelined HGETALL → upsert into redis_timeseries_backup.
        Returns number of keys backed up.
        """
        keys = await _scan_keys(client, TIMESERIES_PATTERNS)
        if not keys:
            logger.debug("Redis backup: no timeseries keys found")
            return 0

        sql = """
            INSERT INTO redis_timeseries_backup (redis_key, hash_data)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                hash_data    = VALUES(hash_data),
                backed_up_at = CURRENT_TIMESTAMP
        """

        backed_up = 0
        skipped   = 0

        for i in range(0, len(keys), _CHUNK_SIZE):
            chunk_keys = keys[i:i + _CHUNK_SIZE]
            results    = await _hgetall_pipeline(client, chunk_keys)

            rows: list[tuple[str, str]] = []
            for key, data in zip(chunk_keys, results):
                if data is None:
                    skipped += 1
                else:
                    rows.append((key, json.dumps(data)))

            if rows:
                await executemany(sql, rows)
                backed_up += len(rows)

            if yield_between_chunks:
                await asyncio.sleep(0)

        logger.debug(
            "Redis backup: {} timeseries keys saved ({} empty skipped)",
            backed_up, skipped,
        )
        return backed_up


# ── Cleanup ───────────────────────────────────────────────────────────────────

class MySQLCleanup:
    """Delete stale rows from MySQL backup tables."""

    @staticmethod
    async def counters() -> int:
        """
        Delete stale rows from redis_counter_backup using pure SQL date parsing.
        Uses STR_TO_DATE(SUBSTRING_INDEX(redis_key, ':', -1), fmt) to extract the
        date suffix from the key name server-side — no Python row fetching needed.
        Returns total rows deleted.
        """
        total = 0

        hourly: list[tuple[str, int]] = [
            ("counter:pokemon_hourly:%",     AppConfig.counter_pokemon_hourly_retention_hours),
            ("counter:tth_pokemon_hourly:%", AppConfig.counter_tth_pokemon_hourly_retention_hours),
            ("counter:raid_hourly:%",        AppConfig.counter_raid_hourly_retention_hours),
            ("counter:invasion_hourly:%",    AppConfig.counter_invasion_hourly_retention_hours),
            ("counter:quest_hourly:%",       AppConfig.counter_quest_hourly_retention_hours),
        ]
        daily: list[tuple[str, int]] = [
            ("counter:pokemon_daily:%",     AppConfig.counter_pokemon_daily_retention_days),
            ("counter:tth_pokemon_daily:%", AppConfig.counter_tth_pokemon_daily_retention_days),
            ("counter:raid_daily:%",        AppConfig.counter_raid_daily_retention_days),
            ("counter:invasion_daily:%",    AppConfig.counter_invasion_daily_retention_days),
            ("counter:quest_daily:%",       AppConfig.counter_quest_daily_retention_days),
        ]

        for prefix, hours in hourly:
            if hours == 0:
                continue
            result = await execute(
                """
                DELETE FROM redis_counter_backup
                WHERE redis_key LIKE %s
                  AND STR_TO_DATE(SUBSTRING_INDEX(redis_key, ':', -1), '%%Y%%m%%d%%H')
                      < NOW() - INTERVAL %s HOUR
                """,
                (prefix, hours),
            )
            total += result.rowcount or 0

        for prefix, days in daily:
            if days == 0:
                continue
            result = await execute(
                """
                DELETE FROM redis_counter_backup
                WHERE redis_key LIKE %s
                  AND STR_TO_DATE(SUBSTRING_INDEX(redis_key, ':', -1), '%%Y%%m%%d')
                      < NOW() - INTERVAL %s DAY
                """,
                (prefix, days),
            )
            total += result.rowcount or 0

        logger.debug("MySQL counter backup cleanup: {} rows deleted", total)
        return total

    @staticmethod
    async def timeseries(backup_interval_sec: int) -> int:
        """
        Delete stale rows from redis_timeseries_backup using backed_up_at as a
        liveness proxy: active Redis keys are refreshed every backup cycle, so a
        row that hasn't been updated in retention + buffer hours is safe to remove.

        buffer = 2 * backup_interval_hours + 1  (prevents false positives during
        slow or back-to-back cycles).
        Returns total rows deleted.
        """
        total = 0
        buffer_hours = (2 * backup_interval_sec // 3600) + 1

        ts_types: list[tuple[str, int]] = [
            ("ts:pokemon:%",      AppConfig.timeseries_pokemon_retention_ms     // 3_600_000),
            ("ts:tth_pokemon:%",  AppConfig.tth_timeseries_retention_ms         // 3_600_000),
            ("ts:raids_total:%",  AppConfig.raid_timeseries_retention_ms        // 3_600_000),
            ("ts:invasion:%",     AppConfig.invasion_timeseries_retention_ms    // 3_600_000),
            ("ts:quests_total:%", AppConfig.quests_timeseries_retention_ms      // 3_600_000),
        ]

        for prefix, retention_hours in ts_types:
            if retention_hours == 0:
                continue
            threshold = retention_hours + buffer_hours
            result = await execute(
                """
                DELETE FROM redis_timeseries_backup
                WHERE redis_key LIKE %s
                  AND backed_up_at < NOW() - INTERVAL %s HOUR
                """,
                (prefix, threshold),
            )
            total += result.rowcount or 0

        logger.debug("MySQL timeseries backup cleanup: {} rows deleted", total)
        return total


# ── Backup provenance ─────────────────────────────────────────────────────────
#
# Which Redis dataset the MySQL backup belongs to. The id lives in Redis itself
# (DATASET_KEY) and is recorded in redis_backup_meta, so it travels with the
# data: a Redis that restarts and reloads its data (RDB/AOF persistence, or an
# RDB restored by hand) keeps the key and still matches, while a Redis that
# lost its data no longer has it. Redis's run_id can't tell these apart - it
# changes on every restart, data or not, and merging the backup into a Redis
# that still holds the data would count everything twice.

META_DATASET_ID = "redis_dataset_id"
DATASET_KEY = "psyduckv2:dataset_id"
DATA_PATTERNS = ("counter:*", "ts:*")


async def redis_dataset_id(client) -> str | None:
    return await client.get(DATASET_KEY)


async def redis_uptime_s(client) -> int | None:
    try:
        info = await client.info("server")
        return int(info.get("uptime_in_seconds"))
    except Exception as e:
        logger.warning("Could not read Redis uptime: {}", e)
        return None


async def get_backup_dataset_id() -> str | None:
    return await fetch_val("SELECT value FROM redis_backup_meta WHERE name = %s", (META_DATASET_ID,))


async def set_backup_dataset_id(dataset_id: str) -> None:
    await execute(
        "INSERT INTO redis_backup_meta (name, value) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE value = VALUES(value)",
        (META_DATASET_ID, dataset_id),
    )


async def mint_dataset_id(client, dataset_id: str | None = None) -> str:
    """Tag the current Redis data with a dataset id and record it as the backup's source."""
    dataset_id = dataset_id or uuid.uuid4().hex
    await set_backup_dataset_id(dataset_id)
    await client.set(DATASET_KEY, dataset_id)
    return dataset_id


async def first_data_key(client) -> str | None:
    """Any counter:/ts: key in Redis, or None when Redis holds no PsyduckV2 data."""
    for pattern in DATA_PATTERNS:
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                return keys[0]
            if cursor == 0:
                break
    return None


# ── Restore (merge) ───────────────────────────────────────────────────────────

async def _merge_rows(client, rows, only_absent: bool = False) -> tuple[int, int]:
    """
    Merge backup rows into Redis without ever overwriting a live value:
      key absent  -> HSET the backed-up hash (restores it)
      key present -> HINCRBY each backed-up field: counts from before a Redis
                     restart are added to counts written since. Every backed-up
                     family is a pure counter, so the two never overlap.
                     Skipped entirely when only_absent is set.
    Returns (keys_restored, keys_merged).
    """
    parsed = []
    for row in rows:
        data = row["hash_data"]
        if isinstance(data, (str, bytes)):
            data = json.loads(data)
        if data:
            parsed.append((row["redis_key"], data))
    if not parsed:
        return 0, 0

    async with client.pipeline(transaction=False) as pipe:
        for key, _ in parsed:
            pipe.exists(key)
        present = await pipe.execute()

    restored = merged = 0
    async with client.pipeline(transaction=False) as pipe:
        for (key, data), is_present in zip(parsed, present):
            if is_present:
                if only_absent:
                    continue
                for field, value in data.items():
                    pipe.hincrby(key, field, int(value))
                merged += 1
            else:
                pipe.hset(key, mapping=data)
                restored += 1
        await pipe.execute()
    return restored, merged


async def _merge_table(client, table: str, older_than_s: int | None, only_absent: bool) -> tuple[int, int]:
    """
    Stream a backup table (bounded memory) and merge it into Redis.

    older_than_s limits the merge to rows last backed up more than that many
    seconds ago - i.e. before the current Redis process started. Newer rows
    were taken from this same Redis, so their counts are already live and
    merging them would count them twice.
    """
    sql, params = f"SELECT redis_key, hash_data FROM {table}", None
    if older_than_s is not None:
        sql += " WHERE backed_up_at < NOW() - INTERVAL %s SECOND"
        params = (int(older_than_s),)
    restored = merged = 0
    async for rows in stream_rows(sql, params, batch=_CHUNK_SIZE):
        r, m = await _merge_rows(client, rows, only_absent)
        restored += r
        merged += m
        await asyncio.sleep(0)   # keep the event loop responsive during long restores
    return restored, merged


async def restore_counters(client, older_than_s: int | None = None, only_absent: bool = False) -> tuple[int, int]:
    restored, merged = await _merge_table(client, "redis_counter_backup", older_than_s, only_absent)
    logger.info("Redis restore: {} counter keys restored from MySQL, {} merged into live keys", restored, merged)
    return restored, merged


async def restore_timeseries(client, older_than_s: int | None = None, only_absent: bool = False) -> tuple[int, int]:
    restored, merged = await _merge_table(client, "redis_timeseries_backup", older_than_s, only_absent)
    logger.info("Redis restore: {} timeseries keys restored from MySQL, {} merged into live keys", restored, merged)
    return restored, merged
