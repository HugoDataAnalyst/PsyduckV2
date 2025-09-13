import asyncio
import random
import aiomysql
from datetime import datetime
from utils.logger import logger
from utils.safe_values import _to_int, _form_str, _username_str
from sql.connect_db import transaction

VALID_IV_BUCKETS = {0, 25, 50, 75, 90, 95, 100}

def _month_year_from_ts(ts: int) -> int:
    return int(datetime.fromtimestamp(ts).strftime("%y%m"))

class PokemonSQLProcessor:
    @staticmethod
    def _row_from_event(d: dict):
        """
        Produce a tuple for tmp table:
        (spawnpoint, lat, lon, pokemon_id, form, iv, level, area_id, day_date, seen_at_str)
        """
        try:
            sp_hex = d.get("spawnpoint")
            if not sp_hex:
                return None
            sp = int(sp_hex, 16)

            lat = d.get("latitude"); lon = d.get("longitude")
            lat = float(lat) if lat is not None else None
            lon = float(lon) if lon is not None else None

            pid   = int(d["pokemon_id"])
            form  = str(d.get("form", "0"))[:15]
            iv    = int(d["iv"])
            level = int(d.get("level"))
            area  = int(d["area_id"])
            ts    = int(d["first_seen"])

            seen_at = datetime.fromtimestamp(ts)               # naive local datetime
            day_date = seen_at.date().isoformat()              # 'YYYY-MM-DD'
            seen_at_str = seen_at.strftime("%Y-%m-%d %H:%M:%S")

            return (sp, lat, lon, pid, form, iv, level, area, day_date, seen_at_str)
        except Exception:
            return None

    @classmethod
    async def bulk_insert_iv_daily_events(cls, data_batch: list[dict], max_retries: int = 8) -> int:
        """
        INSERT IGNORE rows into pokemon_iv_daily_events and keep spawnpoints fresh.
        """
        rows = []
        for d in data_batch:
            t = cls._row_from_event(d)
            if t:
                rows.append(t)
        if not rows:
            return 0

        rows.sort()
        placeholders = "(" + ",".join(["%s"] * 10) + ")"

        attempt = 0
        while attempt < max_retries:
            try:
                async with transaction(isolation="READ COMMITTED", lock_wait_timeout=10, dict_cursor=False) as cur:
                    # 1) temp table
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_ivd (
                          spawnpoint BIGINT NOT NULL,
                          latitude   DOUBLE NULL,
                          longitude  DOUBLE NULL,
                          pokemon_id SMALLINT NOT NULL,
                          form       VARCHAR(15) NOT NULL,
                          iv         SMALLINT NOT NULL,
                          level      TINYINT  NOT NULL,
                          area_id    SMALLINT NOT NULL,
                          day_date   DATE NOT NULL,
                          seen_at    DATETIME NOT NULL,
                          INDEX idx_sp (spawnpoint),
                          INDEX idx_day (day_date),
                          INDEX idx_seen (seen_at)
                        ) ENGINE=InnoDB
                    """)

                    # 2) bulk insert tmp
                    B = 5000
                    for i in range(0, len(rows), B):
                        chunk = rows[i:i+B]
                        flat  = tuple(v for r in chunk for v in r)
                        values = ",".join([placeholders]*len(chunk))
                        await cur.execute(f"INSERT INTO tmp_ivd VALUES {values}", flat)

                    # 3) spawnpoints (insert new + update changed coords)
                    await cur.execute("""
                        INSERT IGNORE INTO spawnpoints (spawnpoint, latitude, longitude)
                        SELECT t.spawnpoint, ANY_VALUE(t.latitude), ANY_VALUE(t.longitude)
                        FROM tmp_ivd t
                        WHERE t.latitude IS NOT NULL AND t.longitude IS NOT NULL
                        GROUP BY t.spawnpoint
                    """)
                    new_sp = cur.rowcount

                    await cur.execute("""
                        UPDATE spawnpoints sp
                        JOIN (
                          SELECT t.spawnpoint,
                                 ANY_VALUE(t.latitude)  AS latitude,
                                 ANY_VALUE(t.longitude) AS longitude
                          FROM tmp_ivd t
                          WHERE t.latitude IS NOT NULL AND t.longitude IS NOT NULL
                          GROUP BY t.spawnpoint
                        ) x ON x.spawnpoint = sp.spawnpoint
                        SET sp.latitude  = x.latitude,
                            sp.longitude = x.longitude
                        WHERE (sp.latitude IS NULL OR sp.longitude IS NULL)
                           OR (sp.latitude <> x.latitude OR sp.longitude <> x.longitude)
                    """)
                    upd_sp = cur.rowcount

                    # 4) final insert (dedupe on PK (day_date, spawnpoint, seen_at))
                    await cur.execute("""
                        INSERT IGNORE INTO pokemon_iv_daily_events
                          (spawnpoint, pokemon_id, form, iv, level, area_id, seen_at, day_date)
                        SELECT t.spawnpoint, t.pokemon_id, t.form, t.iv, t.level, t.area_id, t.seen_at, t.day_date
                        FROM tmp_ivd t
                    """)
                    inserted = cur.rowcount

                    # 5) cleanup
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_ivd")

                logger.info(f"✅ IV daily-events: rows={len(rows)} inserted={inserted} new_sp={new_sp} upd_sp={upd_sp}")
                return inserted

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    attempt += 1
                    backoff = min(2.0, 0.25*attempt) + random.uniform(0, 0.1)
                    logger.warning(f"⚠️ IV daily-events deadlock/timeout; retry {attempt}/{max_retries} in {backoff:.2f}s")
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ IV daily-events DB error: {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ IV daily-events unexpected error: {e}", exc_info=True)
                return 0

        logger.error("❌ IV daily-events: max retries reached")
        return 0

    @staticmethod
    def _parse_shiny_row(d: dict):
        """
        Normalize one shiny payload into a tuple or return None if invalid.

        Returns:
        (username, pokemon_id, form, shiny, area_id, month_year, inc)
        """
        username = _username_str(d.get("username"))
        if not username:
            return None  # username is part of PK; skip if missing/empty

        pokemon_id = _to_int(d.get("pokemon_id"), 0)
        if pokemon_id <= 0:
            return None

        # form may be alphanumeric; keep ASCII only to match table's ascii column
        form = _form_str(d.get("form"))
        if not form:
            form = "0"  # consistent with other tables using "0" default

        shiny = _to_int(d.get("shiny"), 0)
        shiny = 1 if shiny else 0  # clamp to {0,1}

        area_id = _to_int(d.get("area_id"), 0)
        if area_id <= 0:
            return None

        ts = _to_int(d.get("first_seen"), 0)
        if ts <= 0:
            return None
        month_year = _month_year_from_ts(ts)

        inc = _to_int(d.get("increment"), 1)
        if inc <= 0:
            inc = 1

        return (username, pokemon_id, form, shiny, area_id, month_year, inc)

    @classmethod
    async def bulk_upsert_shiny_username_rate_batch(
        cls,
        data_batch: list,
        max_retries: int = 10
    ) -> int:
        """
        Batch upsert shiny username rates using multi-VALUES.
        Unique key: (username, pokemon_id, form, shiny, area_id, month_year)
        """
        # sanitize -> build rows
        rows = []
        for d in data_batch:
            r = cls._parse_shiny_row(d)
            if r is None:
                # Log once per bad record (optional)
                logger.debug(f"⏭️ Skipping invalid shiny row: {d}")
                continue
            rows.append(r)

        if not rows:
            logger.warning("⚠️ No valid aggregated shiny records to upsert.")
            return 0

        # Stable ordering reduces deadlocks
        rows.sort()

        placeholders = "(" + ",".join(["%s"] * 7) + ")"
        collist = "username,pokemon_id,form,shiny,area_id,month_year,total_count"

        BATCH = 2000
        inserted = 0

        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            values_clause = ",".join([placeholders] * len(chunk))
            sql = (
                f"INSERT INTO shiny_username_rates ({collist}) "
                f"VALUES {values_clause} "
                "ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)"
            )
            params = tuple(v for row in chunk for v in row)

            attempt = 0
            while attempt < max_retries:
                try:
                    async with transaction(
                        dict_cursor=False,
                        isolation="READ COMMITTED",
                        lock_wait_timeout=10
                    ) as cur:
                        await cur.execute(sql, params)
                    inserted += len(chunk)
                    break
                except aiomysql.Error as e:
                    code = e.args[0] if e.args else None
                    if code in (1213, 1205):
                        attempt += 1
                        backoff = min(2.0, 0.25 * attempt) + random.uniform(0, 0.05)
                        logger.warning(
                            f"⚠️ shiny upsert {('deadlock' if code==1213 else 'timeout')} "
                            f"on rows {i}-{i+len(chunk)}; retry {attempt}/{max_retries} in {backoff:.2f}s"
                        )
                        await asyncio.sleep(backoff)
                        continue
                    logger.error(f"❌ shiny upsert failed: {e}", exc_info=True)
                    return inserted
                except Exception as e:
                    logger.error(f"❌ shiny upsert unexpected: {e}", exc_info=True)
                    return inserted

            if attempt >= max_retries:
                logger.error(f"❌ shiny upsert exceeded retries on rows {i}-{i+len(chunk)}")
                break

        logger.debug(f"✅ Upserted {inserted} shiny rows (multi-VALUES).")
        return inserted
