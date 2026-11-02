"""
Weather sync from Golbat's own `weather` table (leader only).

Golbat sends a weather webhook only when a cell's condition changes, so a cell
that keeps its weather has no row in weather_daily_events until it changes,
and changes made while PsyduckV2 was down are never sent again. Golbat's
`weather` table always holds every cell's current state; this compares it with
the latest stored row per (cell, area) and writes a row when there is none yet
(seeding) or the condition/warning differs (a missed change).

  * Same path as the webhook: the row goes through WebhookFilter's weather
    handler (polygon overlap -> one entry per area, area-local time) and the
    weather buffer.
  * Time: Golbat's `updated` is when the cell was last read, not when it
    changed. In-game weather changes on the hour, so a synced row starts at
    the hour of `updated` - the latest the change can have happened - and
    always after the cell's previous stored row.
  * Cells Golbat has not refreshed for max_age_hours are skipped: their state
    is too old to write as fact.
"""
from __future__ import annotations

import asyncio
import calendar
import time

import config as AppConfig
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.weather_bulk_buffer import WeatherRedisBuffer
from sql.connect_db import fetch_all
from sql.tasks.golbat_pokestops import get_golbat_mysql_pool
from utils.logger import logger
from utils.s2cell import corners
from webhook.filter_data import WebhookFilter

_GOLBAT_SQL = """
    SELECT id, latitude, longitude, gameplay_condition, warn_weather, severity, wind_direction,
           cloud_level, rain_level, wind_level, snow_level, fog_level, special_effect_level, updated
    FROM weather
    WHERE gameplay_condition IS NOT NULL AND updated >= %s
"""
_LATEST_SQL = """
    SELECT w.s2_cell_id, w.area_id, w.gameplay_condition, w.warn_weather, w.seen_at
    FROM weather_daily_events w
    JOIN (SELECT s2_cell_id, area_id, MAX(seen_at) AS m
          FROM weather_daily_events
          WHERE day_date >= CURDATE() - INTERVAL %s DAY
          GROUP BY s2_cell_id, area_id) x
      ON x.s2_cell_id = w.s2_cell_id AND x.area_id = w.area_id AND x.m = w.seen_at
"""
LOOKBACK_DAYS = 7


async def _golbat_weather(min_updated: int) -> list[dict]:
    pool = await get_golbat_mysql_pool()
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_GOLBAT_SQL, (min_updated,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in await cur.fetchall()]
    finally:
        pool.close()
        await pool.wait_closed()


async def sync_once(geofences: list, max_age_hours: float) -> dict:
    """One comparison pass. Returns counts for the log line."""
    client = await RedisManager().check_redis_connection()
    if not client:
        raise RuntimeError("Redis not reachable")
    # Changes already received by webhook must be in MySQL before comparing
    await WeatherRedisBuffer.force_flush(client)

    now = int(time.time())
    min_updated = now - int(max_age_hours * 3600) if max_age_hours > 0 else 0
    golbat = await _golbat_weather(min_updated)
    latest = {(int(r["s2_cell_id"]), int(r["area_id"])): r for r in await fetch_all(_LATEST_SQL, (LOOKBACK_DAYS,))}

    weather_filter = WebhookFilter({"weather"}, geofences)
    stats = {"golbat_cells": len(golbat), "seeded": 0, "missed_changes": 0}
    async with client.pipeline(transaction=False) as pipe:
        for g in golbat:
            updated = int(g["updated"])
            message = {
                "s2_cell_id": int(g["id"]),
                "gameplay_condition": int(g["gameplay_condition"]),
                "warn_weather": int(g["warn_weather"] or 0),
                "severity": g["severity"], "wind_direction": g["wind_direction"],
                "cloud_level": g["cloud_level"], "rain_level": g["rain_level"], "wind_level": g["wind_level"],
                "snow_level": g["snow_level"], "fog_level": g["fog_level"],
                "special_effect_level": g["special_effect_level"],
                "latitude": float(g["latitude"]), "longitude": float(g["longitude"]),
                "polygon": [list(c) for c in corners(int(g["id"]))],
                "updated": updated - updated % 3600,
            }
            data = await weather_filter.handle_weather_data(message)
            if not data:
                continue
            todo = []
            for area in data["areas"]:
                stored = latest.get((message["s2_cell_id"], int(area["area_id"])))
                if stored is None:
                    stats["seeded"] += 1
                elif (int(stored["gameplay_condition"]), int(stored["warn_weather"])) != \
                        (message["gameplay_condition"], message["warn_weather"]):
                    stats["missed_changes"] += 1
                    # never before the cell's previous row in this area
                    previous = calendar.timegm(stored["seen_at"].timetuple())
                    area = dict(area, updated=max(area["updated"], previous + 1))
                else:
                    continue
                todo.append(area)
            if todo:
                WeatherRedisBuffer.stage(pipe, dict(data, areas=todo))
        await pipe.execute()
    if stats["seeded"] or stats["missed_changes"]:
        await WeatherRedisBuffer.force_flush(client)
    return stats


class WeatherGolbatSync:
    def __init__(self, interval_minutes: int, max_age_hours: float):
        self.interval = max(1, int(interval_minutes)) * 60
        self.max_age_hours = max_age_hours
        self._task: asyncio.Task | None = None

    async def _loop(self):
        await asyncio.sleep(30)   # let the first webhooks and flushes land
        while True:
            try:
                from utils.global_state_manager import GlobalStateManager
                from server_fastapi import global_state
                geofences = await GlobalStateManager.get_geofences() or global_state.geofences
                t0 = time.perf_counter()
                stats = await sync_once(geofences or [], self.max_age_hours)
                line = (f"🌦️ Weather sync from Golbat: {stats['golbat_cells']} fresh cells, "
                        f"{stats['seeded']} seeded, {stats['missed_changes']} missed changes "
                        f"in {time.perf_counter() - t0:.2f}s")
                (logger.success if stats["seeded"] or stats["missed_changes"] else logger.info)(line)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌ Weather sync from Golbat failed: {e}")
            await asyncio.sleep(self.interval)

    async def start(self):
        self._task = asyncio.create_task(self._loop())
        logger.info(f"🚀 Started Weather sync from Golbat every {self.interval // 60} min")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
