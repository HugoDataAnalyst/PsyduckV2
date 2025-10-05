import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from datetime import datetime
import time
from utils.safe_values import _to_int, _to_float, _form_str
from utils.logger import logger
import config as AppConfig

class QuestSQLProcessor:
    @classmethod
    async def bulk_insert_quests_daily_events(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        Split incoming quest events into two temp tables (items vs pokemon),
        upsert pokestops, then insert into:
        - quests_item_daily_events
        - quests_pokemon_daily_events

        Expected per dict:
        pokestop, pokestop_name, latitude, longitude,
        area_id, first_seen (epoch), mode, task_type, kind (0=item,1=pokemon),
        item_id, item_amount, poke_id, poke_form
        """
        if not data_batch:
            return 0

        items_rows = []
        pokemon_rows = []
        for d in data_batch:
            try:
                pokestop      = str(d["pokestop"])
                pokestop_name = str(d.get("pokestop_name", ""))[:255]
                lat           = float(d.get("latitude", 0.0))
                lon           = float(d.get("longitude", 0.0))
                area_id       = int(d["area_id"])
                first_seen    = int(d["first_seen"])
                mode          = int(d["mode"])
                task_type     = int(d["task_type"])
                kind          = int(d["kind"])
                if not pokestop or area_id < 0 or first_seen <= 0 or task_type <= 0:
                    continue

                if kind == 0:
                    item_id  = int(d.get("item_id", 0))
                    item_amt = int(d.get("item_amount", 0) or 1)
                    if item_id <= 0:
                        continue
                    items_rows.append((
                        pokestop, pokestop_name, lat, lon,
                        area_id, first_seen, mode, task_type,
                        item_id, item_amt
                    ))
                else:
                    poke_id   = int(d.get("poke_id", 0))
                    poke_form = str(d.get("poke_form", "") or "0")
                    if poke_id <= 0:
                        continue
                    inc = 1
                    pokemon_rows.append((
                        pokestop, pokestop_name, lat, lon,
                        area_id, first_seen, mode, task_type,
                        poke_id, poke_form, inc
                    ))
            except Exception:
                continue

        if not items_rows and not pokemon_rows:
            return 0

        BATCH = 5000
        attempt = 0

        while attempt < max_retries:
            try:
                async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
                    # 0) ALWAYS create both temp tables so later UNION/selects never fail
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_qide (
                        pokestop      VARCHAR(50)  NOT NULL,
                        pokestop_name VARCHAR(255) NOT NULL,
                        latitude      DOUBLE NOT NULL,
                        longitude     DOUBLE NOT NULL,

                        area_id       SMALLINT UNSIGNED NOT NULL,
                        first_seen    BIGINT NOT NULL,        -- epoch
                        mode          TINYINT  UNSIGNED NOT NULL,
                        task_type     SMALLINT UNSIGNED NOT NULL,

                        item_id       SMALLINT UNSIGNED NOT NULL,
                        item_amount   SMALLINT UNSIGNED NOT NULL,

                        INDEX ix_tmp_qide_p (pokestop),
                        INDEX ix_tmp_qide_s (first_seen)
                        ) ENGINE=InnoDB
                    """)
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_qpde (
                        pokestop      VARCHAR(50)  NOT NULL,
                        pokestop_name VARCHAR(255) NOT NULL,
                        latitude      DOUBLE NOT NULL,
                        longitude     DOUBLE NOT NULL,

                        area_id       SMALLINT UNSIGNED NOT NULL,
                        first_seen    BIGINT NOT NULL,        -- epoch
                        mode          TINYINT  UNSIGNED NOT NULL,
                        task_type     SMALLINT UNSIGNED NOT NULL,

                        poke_id       SMALLINT UNSIGNED NOT NULL,
                        poke_form     VARCHAR(15) NOT NULL,
                        inc           INT      UNSIGNED NOT NULL,

                        INDEX ix_tmp_qpde_p (pokestop),
                        INDEX ix_tmp_qpde_s (first_seen)
                        ) ENGINE=InnoDB
                    """)

                    # 1) Conditionally insert into each temp (they exist even if no rows)
                    if items_rows:
                        ph = "(" + ",".join(["%s"] * 10) + ")"
                        for i in range(0, len(items_rows), BATCH):
                            chunk = items_rows[i:i+BATCH]
                            flat = tuple(v for row in chunk for v in row)
                            vals = ",".join([ph] * len(chunk))
                            await cur.execute(f"INSERT INTO tmp_qide VALUES {vals}", flat)

                    if pokemon_rows:
                        ph = "(" + ",".join(["%s"] * 11) + ")"
                        for i in range(0, len(pokemon_rows), BATCH):
                            chunk = pokemon_rows[i:i+BATCH]
                            flat = tuple(v for row in chunk for v in row)
                            vals = ",".join([ph] * len(chunk))
                            await cur.execute(f"INSERT INTO tmp_qpde VALUES {vals}", flat)

                    # 2) Upsert pokestops using union of both temps (safe if one is empty)
                    await cur.execute("""
                        INSERT IGNORE INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                        SELECT pokestop, ANY_VALUE(pokestop_name), ANY_VALUE(latitude), ANY_VALUE(longitude)
                        FROM (
                            SELECT t.pokestop, t.pokestop_name, t.latitude, t.longitude FROM tmp_qide t
                            UNION ALL
                            SELECT t.pokestop, t.pokestop_name, t.latitude, t.longitude FROM tmp_qpde t
                        ) u
                        GROUP BY pokestop
                    """)
                    new_ps = cur.rowcount

                    await cur.execute("""
                        UPDATE pokestops p
                        JOIN (
                        SELECT pokestop,
                                ANY_VALUE(pokestop_name) AS pokestop_name,
                                ANY_VALUE(latitude)      AS latitude,
                                ANY_VALUE(longitude)     AS longitude
                        FROM (
                            SELECT t.pokestop, t.pokestop_name, t.latitude, t.longitude FROM tmp_qide t
                            UNION ALL
                            SELECT t.pokestop, t.pokestop_name, t.latitude, t.longitude FROM tmp_qpde t
                        ) u
                        GROUP BY pokestop
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

                    # 3) Insert daily rows
                    if items_rows:
                        await cur.execute("""
                            INSERT IGNORE INTO quests_item_daily_events (
                                pokestop, area_id, seen_at, day_date, mode, task_type,
                                item_id, item_amount
                            )
                            SELECT
                                t.pokestop,
                                t.area_id,
                                FROM_UNIXTIME(t.first_seen) AS seen_at,
                                DATE(FROM_UNIXTIME(t.first_seen)) AS day_date,
                                t.mode, t.task_type,
                                t.item_id, t.item_amount
                            FROM tmp_qide t
                        """)

                    if pokemon_rows:
                        await cur.execute("""
                            INSERT INTO quests_pokemon_daily_events (
                                pokestop, area_id, seen_at, day_date, mode, task_type,
                                poke_id, poke_form
                            )
                            SELECT
                                t.pokestop,
                                t.area_id,
                                FROM_UNIXTIME(t.first_seen) AS seen_at,
                                DATE(FROM_UNIXTIME(t.first_seen)) AS day_date,
                                t.mode, t.task_type,
                                t.poke_id, t.poke_form
                            FROM tmp_qpde t
                            GROUP BY
                                t.pokestop, t.area_id, t.first_seen, t.mode, t.task_type,
                                t.poke_id, t.poke_form
                        """)

                    # 4) Cleanup temps regardless of which had data
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_qide")
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_qpde")

                    logger.info(
                        f"🧮 Quests daily | new_ps={new_ps} upd_ps={upd_ps} "
                        f"items={len(items_rows)} pokemon={len(pokemon_rows)}"
                    )
                    return len(items_rows) + len(pokemon_rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ quests daily-events {('deadlock' if code==1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (quests daily-events): {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ Unexpected (quests daily-events): {e}", exc_info=True)
                return 0

        logger.error("❌ quests daily-events: max retries reached")
        return 0
