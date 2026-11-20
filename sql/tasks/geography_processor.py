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

_SAME = " AND ".join(f"c.{f} <=> t.{f}" for f in GEOGRAPHY_FIELDS)
_SAME_UPSERT = " AND ".join(f"cell_geography.{f} <=> VALUES({f})" for f in GEOGRAPHY_FIELDS)


class GeographySQLProcessor:
    @classmethod
    async def upsert_cell_geography(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        Write each S2 level-16 cell's latest game geography to cell_geography through a TEMP table.

        Expects per cell: l16 (int), the GEOGRAPHY_FIELDS (int or None), updated (UTC epoch). A cell whose
        stored geography differs gets a cell_geography_changes row first. Same values on the same UTC day
        change nothing (no write churn). Returns the number of cells in the batch.
        """
        rows = []
        for d in data_batch:
            try:
                updated = int(d["updated"])
                if updated <= 0:
                    continue
                seen = datetime.fromtimestamp(updated, timezone.utc)
                rows.append((int(d["l16"]), *(d.get(k) for k in GEOGRAPHY_FIELDS),
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
                          l16       BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                          {", ".join(f"{f} TINYINT UNSIGNED NULL" for f in GEOGRAPHY_FIELDS)},
                          seen_at   DATETIME NOT NULL,
                          seen_day  DATE NOT NULL
                        ) ENGINE=InnoDB
                    """)

                    ph = "(" + ",".join(["%s"] * 10) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i + BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        await cur.execute(f"INSERT INTO tmp_geo VALUES {','.join([ph] * len(chunk))}", flat)

                    # 1) change log: stored geography different from the incoming one
                    await cur.execute(f"""
                        INSERT IGNORE INTO cell_geography_changes (
                            l16, changed_at,
                            {", ".join(f"old_{f}" for f in GEOGRAPHY_FIELDS)},
                            {", ".join(f"new_{f}" for f in GEOGRAPHY_FIELDS)}
                        )
                        SELECT t.l16, t.seen_at,
                            {", ".join(f"c.{f}" for f in GEOGRAPHY_FIELDS)},
                            {", ".join(f"t.{f}" for f in GEOGRAPHY_FIELDS)}
                        FROM tmp_geo t
                        JOIN cell_geography c ON c.l16 = t.l16
                        WHERE NOT ({_SAME})
                    """)
                    changes = cur.rowcount

                    # 2) cell_geography: assignments run left to right, so geo_changed_at is decided
                    #    before the geography columns are overwritten
                    await cur.execute(f"""
                        INSERT INTO cell_geography (l16, {cols}, geo_first_seen, geo_seen_day, geo_changed_at)
                        SELECT t.l16, {", ".join(f"t.{f}" for f in GEOGRAPHY_FIELDS)}, t.seen_at, t.seen_day, NULL
                        FROM tmp_geo t
                        ON DUPLICATE KEY UPDATE
                            cell_geography.geo_changed_at = IF(NOT ({_SAME_UPSERT}),
                                                VALUES(geo_first_seen), cell_geography.geo_changed_at),
                            {", ".join(f"cell_geography.{f} = VALUES({f})" for f in GEOGRAPHY_FIELDS)},
                            cell_geography.geo_seen_day = VALUES(geo_seen_day)
                    """)
                    affected = cur.rowcount

                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_geo")

                logger.info(f"🗺️ Cell geography | cells={len(rows)} changes={changes} affected={affected} in {time.perf_counter() - t0:.2f}s ⏱️")
                return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ cell geography {('deadlock' if code == 1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (cell geography): {e}")
                raise FlushError(f"cell geography DB error: {e}") from e
            except Exception as e:
                logger.error(f"❌ Unexpected (cell geography): {e}", exc_info=True)
                raise FlushError(f"cell geography unexpected error: {e}") from e

        logger.error("❌ cell geography: max retries reached")
        raise FlushError("cell geography: max retries reached")
