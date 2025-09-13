import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from datetime import datetime
import time
from utils.logger import logger
import config as AppConfig

class InvasionSQLProcessor:
    @classmethod
    async def bulk_insert_invasions_daily_events(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        Fast insert into invasions_daily_events using a TEMP table, with prior upsert of pokestops.

        Expects per event:
          pokestop, pokestop_name, latitude, longitude,
          display_type, character, grunt, confirmed, area_id, first_seen (epoch)
        """
        # Normalize rows
        rows = []
        for d in data_batch:
            try:
                pokestop      = str(d["pokestop"]).strip()
                pokestop_name = str(d.get("pokestop_name", "")).strip()
                latitude      = float(d.get("latitude", 0.0))
                longitude     = float(d.get("longitude", 0.0))

                display_type  = int(d.get("display_type", 0))
                character     = int(d.get("character", 0))
                grunt         = int(d.get("grunt", 0))
                confirmed     = int(d.get("confirmed", 0))
                area_id       = int(d["area_id"])
                first_seen    = int(d["first_seen"])
                if not pokestop or first_seen <= 0:
                    continue

                rows.append((
                    pokestop, pokestop_name[:255], latitude, longitude,
                    display_type, character, grunt, confirmed,
                    area_id, first_seen
                ))
            except Exception:
                continue

        if not rows:
            return 0

        BATCH = 5000
        attempt = 0

        while attempt < max_retries:
            try:
                async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
                    # Temp table
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_ide (
                          pokestop      VARCHAR(50)  NOT NULL,
                          pokestop_name VARCHAR(255) NOT NULL,
                          latitude      DOUBLE NOT NULL,
                          longitude     DOUBLE NOT NULL,

                          display_type  SMALLINT UNSIGNED NOT NULL,
                          `character`   SMALLINT UNSIGNED NOT NULL,
                          grunt         SMALLINT UNSIGNED NOT NULL,
                          confirmed     TINYINT  UNSIGNED NOT NULL,
                          area_id       SMALLINT UNSIGNED NOT NULL,
                          first_seen    BIGINT NOT NULL,

                          INDEX ix_tmp_ide_p (pokestop),
                          INDEX ix_tmp_ide_s (first_seen)
                        ) ENGINE=InnoDB
                    """)

                    ph = "(" + ",".join(["%s"] * 10) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i+BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        vals = ",".join([ph] * len(chunk))
                        await cur.execute(f"INSERT INTO tmp_ide VALUES {vals}", flat)

                    # Upsert pokestops (PK = pokestop)
                    # a) Insert brand-new
                    await cur.execute("""
                        INSERT IGNORE INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                        SELECT
                          t.pokestop,
                          ANY_VALUE(t.pokestop_name),
                          ANY_VALUE(t.latitude),
                          ANY_VALUE(t.longitude)
                        FROM tmp_ide t
                        GROUP BY t.pokestop
                    """)
                    new_ps = cur.rowcount

                    # b) Update changed name/coords
                    await cur.execute("""
                        UPDATE pokestops p
                        JOIN (
                          SELECT
                            t.pokestop,
                            ANY_VALUE(t.pokestop_name) AS pokestop_name,
                            ANY_VALUE(t.latitude)      AS latitude,
                            ANY_VALUE(t.longitude)     AS longitude
                          FROM tmp_ide t
                          GROUP BY t.pokestop
                        ) x ON x.pokestop = p.pokestop
                        SET
                          p.pokestop_name = x.pokestop_name,
                          p.latitude      = x.latitude,
                          p.longitude     = x.longitude
                        WHERE
                          p.pokestop_name <> x.pokestop_name
                          OR p.latitude  <> x.latitude
                          OR p.longitude <> x.longitude
                    """)
                    upd_ps = cur.rowcount

                    # Insert daily events (IGNORE duplicates, they wil be non existant anyway)
                    await cur.execute("""
                        INSERT IGNORE INTO invasions_daily_events (
                            pokestop, display_type, `character`, grunt, confirmed,
                            area_id, seen_at, day_date
                        )
                        SELECT
                            t.pokestop, t.display_type, t.`character`, t.grunt, t.confirmed,
                            t.area_id,
                            FROM_UNIXTIME(t.first_seen) AS seen_at,
                            DATE(FROM_UNIXTIME(t.first_seen)) AS day_date
                        FROM tmp_ide t
                    """)

                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_ide")

                    logger.info(f"🧮 Invasions daily | new_ps={new_ps} upd_ps={upd_ps} in_rows={len(rows)}")
                    return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ invasions daily-events {('deadlock' if code==1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (invasions daily-events): {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ Unexpected (invasions daily-events): {e}", exc_info=True)
                return 0

        logger.error("❌ invasions daily-events: max retries reached")
        return 0

