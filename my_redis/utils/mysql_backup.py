"""
Redis → MySQL backup and restore utilities.

Backs up counter:* and ts:* HASH keys to MySQL so Redis can run with
persistence disabled (REDIS_MYSQL_BACKUPS = true in config.json).

Buffer keys (buffer:*) are intentionally excluded — they are transient
staging data that is always flushed to MySQL before being discarded.
"""

import json
from utils.logger import logger
from sql.connect_db import executemany, fetch_all, fetch_val

# ── Key patterns to back up ───────────────────────────────────────────────────

COUNTER_PATTERNS: list[str] = ["counter:*"]

TIMESERIES_PATTERNS: list[str] = [
    "ts:pokemon:*",
    "ts:tth_pokemon:*",
    "ts:raids_total:*",
    "ts:invasion:*",
    "ts:quests_total:*",
]

_CHUNK_SIZE = 500   # rows per executemany batch


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


async def _hgetall_str(client, key: str) -> dict[str, str] | None:
    """Return HGETALL result as {str: str}, or None if the key is empty/missing."""
    raw = await client.hgetall(key)
    if not raw:
        return None
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in raw.items()
    }


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


async def backup_counters(client) -> int:
    """
    SCAN counter:* → HGETALL → upsert into redis_counter_backup.
    Returns number of keys backed up.
    """
    keys = await _scan_keys(client, COUNTER_PATTERNS)
    if not keys:
        logger.debug("Redis backup: no counter keys found")
        return 0

    batch: list[tuple[str, str]] = []
    skipped = 0
    for key in keys:
        data = await _hgetall_str(client, key)
        if data is None:
            skipped += 1
            continue
        batch.append((key, json.dumps(data)))

    if not batch:
        return 0

    sql = """
        INSERT INTO redis_counter_backup (redis_key, hash_data)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            hash_data    = VALUES(hash_data),
            backed_up_at = CURRENT_TIMESTAMP
    """
    for i in range(0, len(batch), _CHUNK_SIZE):
        await executemany(sql, batch[i:i + _CHUNK_SIZE])

    logger.debug("Redis backup: {} counter keys saved ({} empty skipped)", len(batch), skipped)
    return len(batch)


async def backup_timeseries(client) -> int:
    """
    SCAN ts:* patterns → HGETALL → upsert into redis_timeseries_backup.
    Returns number of keys backed up.
    """
    keys = await _scan_keys(client, TIMESERIES_PATTERNS)
    if not keys:
        logger.debug("Redis backup: no timeseries keys found")
        return 0

    batch: list[tuple[str, str]] = []
    skipped = 0
    for key in keys:
        data = await _hgetall_str(client, key)
        if data is None:
            skipped += 1
            continue
        batch.append((key, json.dumps(data)))

    if not batch:
        return 0

    sql = """
        INSERT INTO redis_timeseries_backup (redis_key, hash_data)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            hash_data    = VALUES(hash_data),
            backed_up_at = CURRENT_TIMESTAMP
    """
    for i in range(0, len(batch), _CHUNK_SIZE):
        await executemany(sql, batch[i:i + _CHUNK_SIZE])

    logger.debug("Redis backup: {} timeseries keys saved ({} empty skipped)", len(batch), skipped)
    return len(batch)


async def backup_all(client) -> None:
    """Back up all counter and timeseries keys. Called by service loop and on shutdown."""
    c = await backup_counters(client)
    t = await backup_timeseries(client)
    logger.info("Redis → MySQL backup complete: {} counter keys, {} timeseries keys", c, t)


# ── Restore ───────────────────────────────────────────────────────────────────

async def restore_counters(client) -> int:
    """
    Load all rows from redis_counter_backup → HSET into Redis.
    Returns number of keys restored.
    """
    rows = await fetch_all("SELECT redis_key, hash_data FROM redis_counter_backup", ())
    if not rows:
        logger.info("Redis restore: redis_counter_backup is empty — nothing to restore")
        return 0

    restored = 0
    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i:i + _CHUNK_SIZE]
        for row in chunk:
            key = row["redis_key"]
            data = row["hash_data"]
            if isinstance(data, str):
                data = json.loads(data)
            if data:
                await client.hset(key, mapping=data)
                restored += 1

    logger.info("Redis restore: {} counter keys restored from MySQL", restored)
    return restored


async def restore_timeseries(client) -> int:
    """
    Load all rows from redis_timeseries_backup → HSET into Redis.
    Returns number of keys restored.
    """
    rows = await fetch_all("SELECT redis_key, hash_data FROM redis_timeseries_backup", ())
    if not rows:
        logger.info("Redis restore: redis_timeseries_backup is empty — nothing to restore")
        return 0

    restored = 0
    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i:i + _CHUNK_SIZE]
        for row in chunk:
            key = row["redis_key"]
            data = row["hash_data"]
            if isinstance(data, str):
                data = json.loads(data)
            if data:
                await client.hset(key, mapping=data)
                restored += 1

    logger.info("Redis restore: {} timeseries keys restored from MySQL", restored)
    return restored

