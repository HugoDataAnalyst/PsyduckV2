import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from datetime import datetime
import time
from utils.logger import logger
import config as AppConfig

class RaidSQLProcessor:
    @staticmethod
    def _parse_row(d: dict):
        """
        Normalize one raid payload into a tuple or return None if invalid.

        Tuple layout (19 fields):
          gym, gym_name, latitude, longitude,
          raid_pokemon, raid_level, raid_form, raid_team,
          raid_costume, raid_is_exclusive, raid_ex_raid_eligible,
          area_id, month_year, inc
        """
        try:
            gym = str(d.get("raid_gym_id") or "").strip()
            if not gym:
                return None

            lat = float(d["raid_latitude"])
            lon = float(d["raid_longitude"])

            area_id = int(d["area_id"])

            raid_pokemon = int(d.get("raid_pokemon", 0))
            raid_level   = int(d.get("raid_level", 0))
            raid_form    = str(d.get("raid_form", "0"))
            raid_team    = int(d.get("raid_team_id", 0))
            raid_costume = str(d.get("raid_costume", "0"))
            raid_is_excl = int(d.get("raid_is_exclusive", 0))
            raid_ex_elig = int(d.get("raid_ex_raid_eligible", 0))

            ts = int(d.get("raid_first_seen"))
            month_year = int(datetime.fromtimestamp(ts).strftime("%y%m"))

            gym_name = str(d.get("raid_gym_name") or "")
            inc = int(d.get("increment", 1))

            return (
                gym, gym_name, lat, lon,
                raid_pokemon, raid_level, raid_form, raid_team,
                raid_costume, raid_is_excl, raid_ex_elig,
                area_id, month_year, inc
            )
        except Exception:
            return None

    @classmethod
    async def bulk_upsert_aggregated_raids_batch(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        TEMP table + set-based upserts:
          - Upsert gyms (natural PK = gym)
          - Aggregate and upsert into aggregated_raids
        Returns number of input rows consumed.
        """
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
                    # 1) TEMP table
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_ar (
                          gym           VARCHAR(50)  NOT NULL,
                          gym_name      VARCHAR(255) NOT NULL,
                          latitude      DOUBLE NOT NULL,
                          longitude     DOUBLE NOT NULL,

                          raid_pokemon  SMALLINT NOT NULL,
                          raid_level    SMALLINT NOT NULL,
                          raid_form     VARCHAR(15) NOT NULL,
                          raid_team     SMALLINT NOT NULL,
                          raid_costume  VARCHAR(15) NOT NULL,
                          raid_is_exclusive      TINYINT NOT NULL,
                          raid_ex_raid_eligible  TINYINT NOT NULL,

                          area_id       SMALLINT NOT NULL,
                          month_year    SMALLINT NOT NULL,
                          inc           INT      NOT NULL,

                          INDEX idx_tmp_ar_gym   (gym),
                          INDEX idx_tmp_ar_month (month_year)
                        ) ENGINE=InnoDB
                    """)

                    # 2) bulk insert into temp
                    placeholders = "(" + ",".join(["%s"] * 14) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i+BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        values = ",".join([placeholders] * len(chunk))
                        await cur.execute(f"INSERT INTO tmp_ar VALUES {values}", flat)

                    # 3) upsert gyms (natural PK = gym)
                    #    a) insert brand-new gyms
                    await cur.execute("""
                        INSERT IGNORE INTO gyms (gym, gym_name, latitude, longitude)
                        SELECT
                          t.gym,
                          ANY_VALUE(t.gym_name),
                          ANY_VALUE(t.latitude),
                          ANY_VALUE(t.longitude)
                        FROM tmp_ar t
                        GROUP BY t.gym
                    """)
                    new_gyms = cur.rowcount

                    #    b) update changed name/coords
                    await cur.execute("""
                        UPDATE gyms g
                        JOIN (
                          SELECT
                            t.gym,
                            ANY_VALUE(t.gym_name) AS gym_name,
                            ANY_VALUE(t.latitude)  AS latitude,
                            ANY_VALUE(t.longitude) AS longitude
                          FROM tmp_ar t
                          GROUP BY t.gym
                        ) x ON x.gym = g.gym
                        SET
                          g.gym_name = x.gym_name,
                          g.latitude  = x.latitude,
                          g.longitude = x.longitude
                        WHERE
                          g.gym_name <> x.gym_name
                          OR g.latitude  <> x.latitude
                          OR g.longitude <> x.longitude
                    """)
                    upd_gyms = cur.rowcount

                    # 4) aggregate + upsert raids
                    await cur.execute("""
                        INSERT INTO aggregated_raids (
                            gym,
                            raid_pokemon, raid_level, raid_form, raid_team,
                            raid_costume, raid_is_exclusive, raid_ex_raid_eligible,
                            area_id, month_year, total_count
                        )
                        SELECT
                            t.gym,
                            t.raid_pokemon, t.raid_level, t.raid_form, t.raid_team,
                            t.raid_costume, t.raid_is_exclusive, t.raid_ex_raid_eligible,
                            t.area_id, t.month_year,
                            SUM(t.inc) AS total_count
                        FROM tmp_ar t
                        GROUP BY
                            t.gym,
                            t.raid_pokemon, t.raid_level, t.raid_form, t.raid_team,
                            t.raid_costume, t.raid_is_exclusive, t.raid_ex_raid_eligible,
                            t.area_id, t.month_year
                        ON DUPLICATE KEY UPDATE
                            total_count = total_count + VALUES(total_count)
                    """)

                    # 5) cleanup temp
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_ar")

                    logger.debug(
                        f"🧮 Raids batch | input_rows={len(rows)}, new_gyms={new_gyms}, updated_gyms={upd_gyms}"
                    )
                    return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):  # deadlock / lock wait timeout
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ raids upsert {('deadlock' if code==1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (raids bulk): {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ Unexpected (raids bulk): {e}", exc_info=True)
                return 0

        logger.error("❌ raids bulk: max retries reached")
        return 0

    @classmethod
    async def upsert_aggregated_raid_from_filtered(cls, filtered_data: dict, increment: int = 1) -> int:
        """
        Single-item wrapper that reuses the bulk path.
        Returns 1 if accepted, 0 otherwise.
        """
        d = dict(filtered_data)
        d["increment"] = increment
        n = await cls.bulk_upsert_aggregated_raids_batch([d], max_retries=8)
        return 1 if n > 0 else 0
