import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from sql.utils.errors import FlushError
from utils.logger import logger

_LEVELS = (
    "gameplay_condition", "warn_weather", "severity", "wind_direction", "cloud_level",
    "rain_level", "wind_level", "snow_level", "fog_level", "special_effect_level",
)


class WeatherSQLProcessor:
    @classmethod
    async def bulk_insert_weather_daily_events(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        Insert weather change events into weather_daily_events through a TEMP table.

        Expects per event: s2_cell_id, the _LEVELS fields, latitude, longitude,
        area_id, updated (epoch, area-local like the other event tables).
        A re-sent event (same cell, same second) is dropped by the primary key.
        """
        rows = []
        for d in data_batch:
            try:
                updated = int(d["updated"])
                if updated <= 0:
                    continue
                rows.append((int(d["s2_cell_id"]), *(int(d.get(k, 0)) for k in _LEVELS),
                             float(d["latitude"]), float(d["longitude"]), int(d["area_id"]), updated))
            except Exception:
                continue

        if not rows:
            return 0

        BATCH = 5000
        attempt = 0

        while attempt < max_retries:
            try:
                async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_wde (
                          s2_cell_id           BIGINT NOT NULL,
                          gameplay_condition   TINYINT UNSIGNED NOT NULL,
                          warn_weather         TINYINT UNSIGNED NOT NULL,
                          severity             TINYINT UNSIGNED NOT NULL,
                          wind_direction       SMALLINT UNSIGNED NOT NULL,
                          cloud_level          TINYINT UNSIGNED NOT NULL,
                          rain_level           TINYINT UNSIGNED NOT NULL,
                          wind_level           TINYINT UNSIGNED NOT NULL,
                          snow_level           TINYINT UNSIGNED NOT NULL,
                          fog_level            TINYINT UNSIGNED NOT NULL,
                          special_effect_level TINYINT UNSIGNED NOT NULL,
                          latitude             DOUBLE NOT NULL,
                          longitude            DOUBLE NOT NULL,
                          area_id              SMALLINT UNSIGNED NOT NULL,
                          updated              BIGINT NOT NULL
                        ) ENGINE=InnoDB
                    """)

                    ph = "(" + ",".join(["%s"] * 15) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i + BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        await cur.execute(f"INSERT INTO tmp_wde VALUES {','.join([ph] * len(chunk))}", flat)

                    await cur.execute("""
                        INSERT IGNORE INTO weather_daily_events (
                            s2_cell_id, area_id, gameplay_condition, warn_weather, severity, wind_direction,
                            cloud_level, rain_level, wind_level, snow_level, fog_level, special_effect_level,
                            latitude, longitude, seen_at, day_date
                        )
                        SELECT
                            t.s2_cell_id, t.area_id, t.gameplay_condition, t.warn_weather, t.severity, t.wind_direction,
                            t.cloud_level, t.rain_level, t.wind_level, t.snow_level, t.fog_level, t.special_effect_level,
                            t.latitude, t.longitude,
                            FROM_UNIXTIME(t.updated) AS seen_at,
                            DATE(FROM_UNIXTIME(t.updated)) AS day_date
                        FROM tmp_wde t
                    """)
                    inserted = cur.rowcount

                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_wde")

                logger.info(f"🌦️ Weather daily | rows={len(rows)} inserted={inserted}")
                return inserted

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ weather daily-events {('deadlock' if code == 1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (weather daily-events): {e}")
                raise FlushError(f"weather daily-events DB error: {e}") from e
            except Exception as e:
                logger.error(f"❌ Unexpected (weather daily-events): {e}", exc_info=True)
                raise FlushError(f"weather daily-events unexpected error: {e}") from e

        logger.error("❌ weather daily-events: max retries reached")
        raise FlushError("weather daily-events: max retries reached")
