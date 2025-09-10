import asyncio
import random
import aiomysql
from sql.connect_db import transaction
from datetime import datetime
import time
from utils.logger import logger
import config as AppConfig

def _to_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return int(v)
    except (TypeError, ValueError):
        return default

def _to_float(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return float(v)
    except (TypeError, ValueError):
        return default
def _form_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    try:
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return s[:15]  # enforce column length



class QuestSQLProcessor:
    @staticmethod
    def _parse_row(d: dict):
        """
        Normalize one quest payload into a tuple for tmp_aq or return None if invalid.

        tmp_aq layout:
          (pokestop, pokestop_name, latitude, longitude,
           area_id, month_year,
           mode,            -- 0=normal, 1=ar
           task_type,       -- ar_type or normal_type based on mode
           reward_kind,     -- 0=item, 1=pokemon
           item_id, item_amount,
           poke_id, poke_form,
           inc)
        """
        pokestop = str(d.get("pokestop_id") or "").strip()
        if not pokestop:
            return None

        lat = _to_float(d.get("latitude"))
        lon = _to_float(d.get("longitude"))
        if lat is None or lon is None:
            return None

        area_id = _to_int(d.get("area_id"), 0)
        if area_id == 0:
            return None

        ts = _to_int(d.get("first_seen"), 0)
        if ts <= 0:
            return None
        month_year = int(datetime.fromtimestamp(ts).strftime("%y%m"))

        name = (d.get("pokestop_name") or "").strip()
        inc  = _to_int(d.get("increment"), 1)
        if inc <= 0:
            inc = 1

        # Determine mode (AR vs NORMAL)
        ar_type     = _to_int(d.get("ar_type"), 0)
        normal_type = _to_int(d.get("normal_type"), 0)

        has_any_ar_reward = (
            _to_int(d.get("reward_ar_type"), 0) > 0 or
            _to_int(d.get("reward_ar_item_id"), 0) > 0 or
            _to_int(d.get("reward_ar_item_amount"), 0) > 0 or
            _to_int(d.get("reward_ar_poke_id"), 0) > 0 or
            bool(_form_str(d.get("reward_ar_poke_form")))
        )

        if ar_type > 0 or has_any_ar_reward:
            mode = 1
            task_type = ar_type
        else:
            mode = 0
            task_type = normal_type

        if task_type == 0:
            # no valid task
            return None

        # Determine reward kind + values
        poke_id   = _to_int(d.get("reward_ar_poke_id" if mode == 1 else "reward_normal_poke_id"), 0)
        poke_form = _form_str(d.get("reward_ar_poke_form" if mode == 1 else "reward_normal_poke_form"))

        item_id     = _to_int(d.get("reward_ar_item_id" if mode == 1 else "reward_normal_item_id"), 0)
        item_amount = _to_int(d.get("reward_ar_item_amount" if mode == 1 else "reward_normal_item_amount"), 0)

        if poke_id > 0:
            reward_kind = 1
            # ensure consistency
            item_id = 0
            item_amount = 0
            if not poke_form:
                poke_form = "0"
        elif item_id > 0:
            reward_kind = 0
            poke_id = 0
            poke_form = ""
            if item_amount <= 0:
                item_amount = 1
        else:
            # no usable reward
            return None

        return (
            pokestop, name, lat, lon,
            area_id, month_year,
            mode, task_type,
            reward_kind,
            item_id, item_amount,
            poke_id, poke_form,
            inc
        )

    @classmethod
    async def bulk_upsert_aggregated_quests_batch(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        rows = []
        dropped = 0
        for d in data_batch:
            r = cls._parse_row(d)
            if r is None:
                dropped += 1
            else:
                rows.append(r)

        if dropped:
            logger.debug(f"🧪 quests: parsed={len(rows)}, dropped={dropped}")
        if not rows:
            return 0

        BATCH = 5000
        attempt = 0

        while attempt < max_retries:
            try:
                async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
                    # 1) temp table
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_aq (
                          pokestop       VARCHAR(50)  NOT NULL,
                          pokestop_name  VARCHAR(255) NOT NULL,
                          latitude       DOUBLE NOT NULL,
                          longitude      DOUBLE NOT NULL,

                          area_id        SMALLINT NOT NULL,
                          month_year     SMALLINT NOT NULL,

                          mode           TINYINT  NOT NULL,  -- 0=normal,1=ar
                          task_type      SMALLINT NOT NULL,

                          reward_kind    TINYINT  NOT NULL,  -- 0=item,1=pokemon
                          item_id        SMALLINT NOT NULL,
                          item_amount    SMALLINT NOT NULL,
                          poke_id        SMALLINT NOT NULL,
                          poke_form      VARCHAR(15) NOT NULL,

                          inc            INT      NOT NULL,

                          INDEX idx_tmp_aq_pokestop (pokestop),
                          INDEX idx_tmp_aq_month    (month_year)
                        ) ENGINE=InnoDB
                    """)

                    # 2) bulk insert
                    placeholders = "(" + ",".join(["%s"] * 14) + ")"
                    inserted_tmp = 0
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i+BATCH]
                        flat = tuple(v for row in chunk for v in row)
                        values = ",".join([placeholders] * len(chunk))
                        await cur.execute(f"INSERT INTO tmp_aq VALUES {values}", flat)
                        inserted_tmp += len(chunk)

                    # 3) pokestops upsert (natural PK= pokestop)
                    await cur.execute("""
                        INSERT IGNORE INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                        SELECT
                          t.pokestop,
                          ANY_VALUE(t.pokestop_name),
                          ANY_VALUE(t.latitude),
                          ANY_VALUE(t.longitude)
                        FROM tmp_aq t
                        GROUP BY t.pokestop
                    """)
                    new_ps = cur.rowcount

                    await cur.execute("""
                        UPDATE pokestops p
                        JOIN (
                          SELECT
                            t.pokestop,
                            ANY_VALUE(t.pokestop_name) AS pokestop_name,
                            ANY_VALUE(t.latitude)      AS latitude,
                            ANY_VALUE(t.longitude)     AS longitude
                          FROM tmp_aq t
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

                    # 4a) items aggregate
                    await cur.execute("""
                        INSERT INTO aggregated_quests_item (
                          pokestop, area_id, month_year, mode, task_type,
                          item_id, item_amount, total_count
                        )
                        SELECT
                          t.pokestop, t.area_id, t.month_year, t.mode, t.task_type,
                          t.item_id, t.item_amount,
                          SUM(t.inc) AS total_count
                        FROM tmp_aq t
                        WHERE t.reward_kind = 0
                        GROUP BY
                          t.pokestop, t.area_id, t.month_year, t.mode, t.task_type,
                          t.item_id, t.item_amount
                        ON DUPLICATE KEY UPDATE
                          total_count = total_count + VALUES(total_count)
                    """)
                    agg_items_rc = cur.rowcount

                    # 4b) pokemon aggregate
                    await cur.execute("""
                        INSERT INTO aggregated_quests_pokemon (
                          pokestop, area_id, month_year, mode, task_type,
                          poke_id, poke_form, total_count
                        )
                        SELECT
                          t.pokestop, t.area_id, t.month_year, t.mode, t.task_type,
                          t.poke_id, t.poke_form,
                          SUM(t.inc) AS total_count
                        FROM tmp_aq t
                        WHERE t.reward_kind = 1
                        GROUP BY
                          t.pokestop, t.area_id, t.month_year, t.mode, t.task_type,
                          t.poke_id, t.poke_form
                        ON DUPLICATE KEY UPDATE
                          total_count = total_count + VALUES(total_count)
                    """)
                    agg_poke_rc = cur.rowcount

                    # cleanup temp
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_aq")

                    logger.debug(
                        f"🧮 Quests batch | in={len(rows)} dropped={dropped} tmp={inserted_tmp} "
                        f"new_ps={new_ps} upd_ps={upd_ps} items_rc={agg_items_rc} poke_rc={agg_poke_rc}"
                    )
                    return len(rows)

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ quests upsert {('deadlock' if code==1213 else 'timeout')}, "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ DB error (quests bulk): {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ Unexpected (quests bulk): {e}", exc_info=True)
                return 0

        logger.error("❌ quests bulk: max retries reached")
        return 0

    @classmethod
    async def upsert_aggregated_quest_from_filtered(cls, filtered_data: dict, increment: int = 1) -> int:
        d = dict(filtered_data)
        d["increment"] = increment
        n = await cls.bulk_upsert_aggregated_quests_batch([d], max_retries=8)
        return 1 if n > 0 else 0
