import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from datetime import datetime
import time
from utils.logger import logger
import config as AppConfig

class RaidSQLProcessor:
    @classmethod
    async def bulk_insert_raid_daily_events(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        Fast path to insert many raid rows into raids_daily_events using a TEMP table.
        Also upserts into gyms (name + coords).

        Expected keys:
          gym, gym_name, latitude, longitude,
          raid_pokemon, raid_form, raid_level, raid_team, raid_costume,
          raid_is_exclusive, raid_ex_raid_eligible, area_id, first_seen (epoch)
        """
        rows = []
        for d in data_batch:
            try:
                rows.append((
                    str(d["gym"]),
                    str(d.get("gym_name", ""))[:255],
                    float(d.get("latitude", 0.0)),
                    float(d.get("longitude", 0.0)),
                    int(d["raid_pokemon"]),
                    str(d.get("raid_level", "0")),
                    int(d.get("raid_form", 0)),
                    int(d.get("raid_team", 0)),
                    str(d.get("raid_costume", "0")),
                    int(d.get("raid_is_exclusive", 0)),
                    int(d.get("raid_ex_raid_eligible", 0)),
                    int(d["area_id"]),
                    int(d["first_seen"]),
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
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_rde (
                          gym                 VARCHAR(50)  NOT NULL,
                          gym_name            VARCHAR(255) NOT NULL,
                          latitude            DOUBLE NOT NULL,
                          longitude           DOUBLE NOT NULL,

                          raid_pokemon        SMALLINT UNSIGNED NOT NULL,
                          raid_level          SMALLINT UNSIGNED NOT NULL,
                          raid_form           VARCHAR(15) NOT NULL,
                          raid_team           SMALLINT UNSIGNED NOT NULL,
                          raid_costume        VARCHAR(15) NOT NULL,
                          raid_is_exclusive   TINYINT  UNSIGNED NOT NULL,
                          raid_ex_raid_eligible TINYINT UNSIGNED NOT NULL,
                          area_id             SMALLINT UNSIGNED NOT NULL,
                          first_seen_epoch    BIGINT NOT NULL,

                          INDEX idx_tmp_rde_gym (gym),
                          INDEX idx_tmp_rde_seen (first_seen_epoch)
                        ) ENGINE=InnoDB
                    """)

                    ph = "(" + ",".join(["%s"] * 13) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i+BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        vals = ",".join([ph] * len(chunk))
                        await cur.execute(f"INSERT INTO tmp_rde VALUES {vals}", flat)

                    # Upsert gyms
                    await cur.execute("""
                        INSERT IGNORE INTO gyms (gym, gym_name, latitude, longitude)
                        SELECT
                          t.gym,
                          ANY_VALUE(t.gym_name),
                          ANY_VALUE(t.latitude),
                          ANY_VALUE(t.longitude)
                        FROM tmp_rde t
                        GROUP BY t.gym
                    """)
                    new_gyms = cur.rowcount

                    await cur.execute("""
                        UPDATE gyms g
                        JOIN (
                          SELECT
                            t.gym,
                            ANY_VALUE(t.gym_name) AS gym_name,
                            ANY_VALUE(t.latitude)  AS latitude,
                            ANY_VALUE(t.longitude) AS longitude
                          FROM tmp_rde t
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

                    # Insert daily rows
                    await cur.execute("""
                        INSERT INTO raids_daily_events (
                            gym,
                            raid_pokemon, raid_level, raid_form, raid_team,
                            raid_costume, raid_is_exclusive, raid_ex_raid_eligible,
                            area_id, seen_at, day_date
                        )
                        SELECT
                            t.gym,
                            t.raid_pokemon, t.raid_level, t.raid_form, t.raid_team,
                            t.raid_costume, t.raid_is_exclusive, t.raid_ex_raid_eligible,
                            t.area_id,
                            FROM_UNIXTIME(t.first_seen_epoch) AS seen_at,
                            DATE(FROM_UNIXTIME(t.first_seen_epoch)) AS day_date
                        FROM tmp_rde t
                        ON DUPLICATE KEY UPDATE
                            raid_pokemon        = VALUES(raid_pokemon),
                            raid_level          = VALUES(raid_level),
                            raid_form           = VALUES(raid_form),
                            raid_team           = VALUES(raid_team),
                            raid_costume        = VALUES(raid_costume),
                            raid_is_exclusive   = VALUES(raid_is_exclusive),
                            raid_ex_raid_eligible = VALUES(raid_ex_raid_eligible),
                            area_id             = VALUES(area_id)
                    """)

                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_rde")
                    logger.info(f"🧮 Raids daily | new_gyms={new_gyms} upd_gyms={upd_gyms} in_rows={len(rows)}")
                    return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):  # deadlock / lock wait timeout
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ raids daily-events insert {('deadlock' if code==1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (raids daily-events): {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ Unexpected (raids daily-events): {e}", exc_info=True)
                return 0

        logger.error("❌ raids daily-events: max retries reached")
        return 0
