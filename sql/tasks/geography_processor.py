import asyncio
import random
import time
from datetime import datetime, timezone
import aiomysql
from sql.connect_db import transaction
from sql.utils.errors import FlushError
from utils.logger import logger

# Order of the geography values in a buffer entry and in the temp table
GEOGRAPHY_FIELDS = ("landform", "moisture", "landcover", "temperature", "features", "park_status", "settlement")

_SAME = " AND ".join(f"spawnpoints.{c} <=> t.{c}" for c in GEOGRAPHY_FIELDS)
_SAME_UPSERT = " AND ".join(f"spawnpoints.{c} <=> VALUES({c})" for c in GEOGRAPHY_FIELDS)


class GeographySQLProcessor:
    @classmethod
    async def upsert_spawnpoint_geography(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        Write each spawnpoint's latest game geography onto its spawnpoints row through a TEMP table.

        Expects per spawnpoint: spawnpoint (int), the GEOGRAPHY_FIELDS (int or None), latitude, longitude,
        updated (UTC epoch). A spawnpoint whose stored geography differs gets a spawnpoint_geography_changes
        row first. Same values on the same UTC day change nothing (no write churn); a missing row is created
        with the event's coords. Returns the number of spawnpoints in the batch.
        """
        rows = []
        for d in data_batch:
            try:
                updated = int(d["updated"])
                if updated <= 0:
                    continue
                seen = datetime.fromtimestamp(updated, timezone.utc)
                rows.append((int(d["spawnpoint"]), float(d["latitude"]), float(d["longitude"]),
                             *(d.get(k) for k in GEOGRAPHY_FIELDS),
                             seen.strftime("%Y-%m-%d %H:%M:%S"), seen.date().isoformat()))
            except Exception:
                continue

        if not rows:
            return 0
        rows.sort()

        BATCH = 5000
        attempt = 0
        cols = ", ".join(GEOGRAPHY_FIELDS)

        while attempt < max_retries:
            try:
                t0 = time.perf_counter()
                async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
                    await cur.execute(f"""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geo (
                          spawnpoint  BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                          latitude    DOUBLE NOT NULL,
                          longitude   DOUBLE NOT NULL,
                          {", ".join(f"{c} TINYINT UNSIGNED NULL" for c in GEOGRAPHY_FIELDS)},
                          seen_at     DATETIME NOT NULL,
                          seen_day    DATE NOT NULL
                        ) ENGINE=InnoDB
                    """)

                    ph = "(" + ",".join(["%s"] * 12) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i + BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        await cur.execute(f"INSERT INTO tmp_geo VALUES {','.join([ph] * len(chunk))}", flat)

                    # 1) change log: stored geography present and different from the incoming one
                    await cur.execute(f"""
                        INSERT IGNORE INTO spawnpoint_geography_changes (
                            spawnpoint, changed_at,
                            {", ".join(f"old_{c}" for c in GEOGRAPHY_FIELDS)},
                            {", ".join(f"new_{c}" for c in GEOGRAPHY_FIELDS)}
                        )
                        SELECT t.spawnpoint, t.seen_at,
                            {", ".join(f"spawnpoints.{c}" for c in GEOGRAPHY_FIELDS)},
                            {", ".join(f"t.{c}" for c in GEOGRAPHY_FIELDS)}
                        FROM tmp_geo t
                        JOIN spawnpoints ON spawnpoints.spawnpoint = t.spawnpoint
                        WHERE spawnpoints.temperature IS NOT NULL AND NOT ({_SAME})
                    """)
                    changes = cur.rowcount

                    # 2) spawnpoints: assignments run left to right, so geo_changed_at is decided
                    #    before the geography columns are overwritten
                    await cur.execute(f"""
                        INSERT INTO spawnpoints (spawnpoint, latitude, longitude, {cols},
                                                 geo_first_seen, geo_seen_day, geo_changed_at)
                        SELECT t.spawnpoint, t.latitude, t.longitude, {", ".join(f"t.{c}" for c in GEOGRAPHY_FIELDS)},
                               t.seen_at, t.seen_day, NULL
                        FROM tmp_geo t
                        ON DUPLICATE KEY UPDATE
                            spawnpoints.geo_changed_at = IF(spawnpoints.temperature IS NOT NULL AND NOT ({_SAME_UPSERT}),
                                                VALUES(geo_first_seen), spawnpoints.geo_changed_at),
                            spawnpoints.geo_first_seen = COALESCE(spawnpoints.geo_first_seen, VALUES(geo_first_seen)),
                            {", ".join(f"spawnpoints.{c} = VALUES({c})" for c in GEOGRAPHY_FIELDS)},
                            spawnpoints.geo_seen_day = VALUES(geo_seen_day)
                    """)
                    affected = cur.rowcount

                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_geo")

                logger.info(f"🗺️ Spawnpoint geography | spawnpoints={len(rows)} changes={changes} affected={affected} in {time.perf_counter() - t0:.2f}s ⏱️")
                return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ spawnpoint geography {('deadlock' if code == 1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (spawnpoint geography): {e}")
                raise FlushError(f"spawnpoint geography DB error: {e}") from e
            except Exception as e:
                logger.error(f"❌ Unexpected (spawnpoint geography): {e}", exc_info=True)
                raise FlushError(f"spawnpoint geography unexpected error: {e}") from e

        logger.error("❌ spawnpoint geography: max retries reached")
        raise FlushError("spawnpoint geography: max retries reached")
