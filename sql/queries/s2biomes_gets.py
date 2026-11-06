"""Raw rows for S2Biomes (biome and nest analysis): spawns and weather of one day partition.

Rows are read as stored: seen_at and day_date are area-local wall time (see webhook/filter_data.py), so
`since` / `until` are area-local too. Spawns are paged by the primary key (day_date, spawnpoint, seen_at):
the next page starts after the last row of the previous one, so every page is an index range scan.

These exports share the event loop with webhook processing, so the per-row work is kept small: tuple rows
(no dicts), seen_at formatted by MariaDB (no per-row strftime), batches of BATCH rows.
"""
from datetime import date, datetime
from typing import AsyncIterator, Optional

from sql.connect_db import fetch_all, stream_rows

SPAWN_COLUMNS = ("spawnpoint", "pokemon_id", "form", "seen_at", "encounter_id", "weather")
WEATHER_COLUMNS = ("s2_cell_id", "area_id", "gameplay_condition", "seen_at")
SEEN_AT = "DATE_FORMAT(seen_at, '%%Y-%%m-%%d %%H:%%i:%%s')"  # %% escapes the driver's parameter marker
# long pages to slow clients: give the server-side cursor time (the default net_write_timeout is 60 s)
STREAM_SESSION = ("SET SESSION net_write_timeout = 600",)
BATCH = 1000  # rows formatted per step: short slices of event-loop time


async def stream_spawns(
    day: date,
    since: Optional[datetime],
    until: Optional[datetime],
    after: Optional[tuple[int, datetime]],
    limit: int,
) -> AsyncIterator[list[dict]]:
    """Spawn rows of one day in primary-key order, at most `limit`, after the `after` (spawnpoint, seen_at) row:
    tuples in SPAWN_COLUMNS order, seen_at as 'YYYY-MM-DD HH:MM:SS'."""
    columns = ", ".join(SEEN_AT if c == "seen_at" else c for c in SPAWN_COLUMNS)
    sql = [f"SELECT {columns} FROM pokemon_iv_daily_events WHERE day_date = %s"]
    params: list = [day]
    if since:
        sql.append("AND seen_at >= %s")
        params.append(since)
    if until:
        sql.append("AND seen_at < %s")
        params.append(until)
    if after:
        sql.append("AND (spawnpoint > %s OR (spawnpoint = %s AND seen_at > %s))")
        params += [after[0], after[0], after[1]]
    sql.append("ORDER BY spawnpoint, seen_at LIMIT %s")
    params.append(limit)
    async for rows in stream_rows(" ".join(sql), params, batch=BATCH, session_sql=STREAM_SESSION, tuples=True):
        yield rows


async def stream_weather(day: date) -> AsyncIterator[list[dict]]:
    """Weather condition changes of one day (one row per S2 L10 cell, area and change), tuples in WEATHER_COLUMNS order."""
    columns = ", ".join(SEEN_AT if c == "seen_at" else c for c in WEATHER_COLUMNS)
    sql = f"SELECT {columns} FROM weather_daily_events WHERE day_date = %s"
    async for rows in stream_rows(sql, [day], batch=BATCH, session_sql=STREAM_SESSION, tuples=True):
        yield rows


async def fetch_days(table: str) -> list[dict]:
    """Days with rows in a daily-partitioned table, from partition metadata (approximate counts, no table scan)."""
    rows = await fetch_all(
        """
        SELECT PARTITION_NAME AS name, TABLE_ROWS AS approx_rows
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND PARTITION_NAME REGEXP '^p[0-9]{8}$'
        ORDER BY PARTITION_NAME
        """,
        [table],
    )
    return [
        {"day": f"{r['name'][1:5]}-{r['name'][5:7]}-{r['name'][7:9]}", "approx_rows": int(r["approx_rows"] or 0)}
        for r in rows
        if r["approx_rows"]
    ]
