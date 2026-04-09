"""
Redis → MySQL backup and restore utilities.

Backs up counter:* and ts:* HASH keys to MySQL so Redis can run with
persistence disabled (REDIS_MYSQL_BACKUPS = true in config.json).

Buffer keys (buffer:*) are intentionally excluded — they are transient
staging data that is always flushed to MySQL before being discarded.

Performance:
    HGETALL calls are pipelined in chunks of _CHUNK_SIZE, so we send
    one round trip per chunk instead of one per key.  HSET restores are
    pipelined the same way.  asyncio.sleep(0) is inserted between chunks
    during periodic backups so webhook coroutines keep running.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
import config as AppConfig
from utils.logger import logger
from sql.connect_db import execute, executemany, fetch_all, fetch_val

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


# ── Backup ────────────────────────────────────────────────────────────────────

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


async def backup_counters(client, *, yield_between_chunks: bool = True) -> int:
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


async def backup_timeseries(client, *, yield_between_chunks: bool = True) -> int:
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


async def backup_all(client, *, yield_between_chunks: bool = True) -> None:
    """Back up all counter and timeseries keys concurrently. Called by service loop and on shutdown."""
    c, t = await asyncio.gather(
        backup_counters(client, yield_between_chunks=yield_between_chunks),
        backup_timeseries(client, yield_between_chunks=yield_between_chunks),
    )
    logger.info("Redis → MySQL backup complete: {} counter keys, {} timeseries keys", c, t)


# ── MySQL backup cleanup ──────────────────────────────────────────────────────

async def _cleanup_counter_backup() -> int:
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


async def _cleanup_timeseries_backup(backup_interval_sec: int) -> int:
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


async def cleanup_mysql_backup(backup_interval_sec: int = 3600) -> tuple[int, int]:
    """
    Delete stale rows from both backup tables before a backup cycle.
    Should be called immediately before backup_all() so tables stay compact
    and always mirror the current Redis retention windows.

    Returns (counter_rows_deleted, timeseries_rows_deleted).
    """
    c = await _cleanup_counter_backup()
    t = await _cleanup_timeseries_backup(backup_interval_sec)
    logger.info("MySQL backup cleanup: {} counter + {} timeseries rows deleted", c, t)
    return c, t


# ── Restore ───────────────────────────────────────────────────────────────────

async def restore_counters(client) -> int:
    """
    Load all rows from redis_counter_backup → pipelined HSET into Redis.
    Returns number of keys restored.
    """
    rows = await fetch_all("SELECT redis_key, hash_data FROM redis_counter_backup", ())
    if not rows:
        logger.info("Redis restore: redis_counter_backup is empty — nothing to restore")
        return 0

    restored = 0
    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i:i + _CHUNK_SIZE]
        pipe  = client.pipeline(transaction=False)
        for row in chunk:
            data = row["hash_data"]
            if isinstance(data, str):
                data = json.loads(data)
            if data:
                pipe.hset(row["redis_key"], mapping=data)
                restored += 1
        await pipe.execute()

    logger.info("Redis restore: {} counter keys restored from MySQL", restored)
    return restored


async def restore_timeseries(client) -> int:
    """
    Load all rows from redis_timeseries_backup → pipelined HSET into Redis.
    Returns number of keys restored.
    """
    rows = await fetch_all("SELECT redis_key, hash_data FROM redis_timeseries_backup", ())
    if not rows:
        logger.info("Redis restore: redis_timeseries_backup is empty — nothing to restore")
        return 0

    restored = 0
    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i:i + _CHUNK_SIZE]
        pipe  = client.pipeline(transaction=False)
        for row in chunk:
            data = row["hash_data"]
            if isinstance(data, str):
                data = json.loads(data)
            if data:
                pipe.hset(row["redis_key"], mapping=data)
                restored += 1
        await pipe.execute()

    logger.info("Redis restore: {} timeseries keys restored from MySQL", restored)
    return restored
