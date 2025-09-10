import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from datetime import datetime
import time
from utils.logger import logger
import config as AppConfig

class InvasionSQLProcessor:
    @staticmethod
    def _parse_row(d: dict):
        """
        Validate & normalize one filtered invasion payload.
        Returns tuple or None.
        tuple layout:
          (pokestop, pokestop_name, latitude, longitude,
           display_type, character, grunt, confirmed,
           area_id, month_year, inc)
        """
        try:
            pokestop = str(d.get("invasion_pokestop_id") or "").strip()
            if not pokestop:
                return None

            lat = float(d["invasion_latitude"])
            lon = float(d["invasion_longitude"])

            area_id = int(d["area_id"])

            display_type = int(d.get("invasion_type", 0))
            character    = int(d.get("invasion_character", 0))
            grunt        = int(d.get("invasion_grunt_type", 0))
            confirmed    = int(d.get("invasion_confirmed", 0))

            ts = int(d.get("invasion_first_seen"))
            month_year = int(datetime.fromtimestamp(ts).strftime("%y%m"))

            name = str(d.get("invasion_pokestop_name") or "")
            inc = int(d.get("increment", 1))

            return (
                pokestop, name, lat, lon,
                display_type, character, grunt, confirmed,
                area_id, month_year, inc
            )
        except Exception:
            return None

    @classmethod
    async def bulk_upsert_aggregated_invasions_batch(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        TEMP table + set-based upserts.
        Returns number of input rows consumed (not rows inserted).
        """
        # Build clean rows
        rows = []
        for d in data_batch:
            r = cls._parse_row(d)
            if r is not None:
                rows.append(r)
        if not rows:
            return 0

        BATCH = 5000
        attempt = 0

        while attempt < max_retries:
            try:
                async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
                    # 1) temp table
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_ai (
                          pokestop     VARCHAR(50) NOT NULL,
                          pokestop_name VARCHAR(255) NOT NULL,
                          latitude     DOUBLE NOT NULL,
                          longitude    DOUBLE NOT NULL,
                          display_type SMALLINT NOT NULL,
                          `character`  SMALLINT NOT NULL,
                          grunt        SMALLINT NOT NULL,
                          confirmed    TINYINT  NOT NULL,
                          area_id      SMALLINT NOT NULL,
                          month_year   SMALLINT NOT NULL,
                          inc          INT      NOT NULL,
                          INDEX idx_tmp_ai_pokestop (pokestop),
                          INDEX idx_tmp_ai_month    (month_year)
                        ) ENGINE=InnoDB
                    """)

                    # 2) bulk insert into tmp
                    placeholders = "(" + ",".join(["%s"] * 11) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i+BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        values = ",".join([placeholders] * len(chunk))
                        await cur.execute(f"INSERT INTO tmp_ai VALUES {values}", flat)

                    # 3) upsert pokestops (natural PK = pokestop)
                    #    a) insert brand-new
                    await cur.execute("""
                        INSERT IGNORE INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                        SELECT
                          t.pokestop,
                          ANY_VALUE(t.pokestop_name),
                          ANY_VALUE(t.latitude),
                          ANY_VALUE(t.longitude)
                        FROM tmp_ai t
                        GROUP BY t.pokestop
                    """)
                    new_ps = cur.rowcount

                    #    b) update coords/name when changed
                    await cur.execute("""
                        UPDATE pokestops p
                        JOIN (
                          SELECT
                            t.pokestop,
                            ANY_VALUE(t.pokestop_name) AS pokestop_name,
                            ANY_VALUE(t.latitude)      AS latitude,
                            ANY_VALUE(t.longitude)     AS longitude
                          FROM tmp_ai t
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

                    # 4) aggregate & upsert invasions
                    await cur.execute("""
                        INSERT INTO aggregated_invasions
                          (pokestop, display_type, `character`, grunt, confirmed,
                           area_id, month_year, total_count)
                        SELECT
                          t.pokestop, t.display_type, t.`character`, t.grunt, t.confirmed,
                          t.area_id, t.month_year, SUM(t.inc) AS total_count
                        FROM tmp_ai t
                        GROUP BY
                          t.pokestop, t.display_type, t.`character`, t.grunt, t.confirmed,
                          t.area_id, t.month_year
                        ON DUPLICATE KEY UPDATE
                          total_count = total_count + VALUES(total_count)
                    """)

                    # 5) cleanup temp (committed by context manager)
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_ai")

                    logger.debug(
                        f"🧮 Invasions batch | input_rows={len(rows)}, new_pokestops={new_ps}, updated_pokestops={upd_ps}"
                    )
                    return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):  # deadlock / lock wait
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ invasions upsert {('deadlock' if code==1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (invasions bulk): {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ Unexpected (invasions bulk): {e}", exc_info=True)
                return 0

        logger.error("❌ invasions bulk: max retries reached")
        return 0

    @classmethod
    async def upsert_aggregated_invasion_from_filtered(cls, filtered_data: dict, increment: int = 1) -> int:
        """
        Single-item wrapper that reuses the bulk path.
        Returns 1 if accepted, 0 otherwise.
        """
        d = dict(filtered_data)
        d["increment"] = increment
        n = await cls.bulk_upsert_aggregated_invasions_batch([d], max_retries=8)
        return 1 if n > 0 else 0
