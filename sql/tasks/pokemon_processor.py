import asyncio
import random
import aiomysql
from datetime import datetime
from utils.logger import logger
from sql.connect_db import transaction

VALID_IV_BUCKETS = {0, 25, 50, 75, 90, 95, 100}

def _to_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return int(v)
    except (TypeError, ValueError):
        return default

def _form_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    # keep ASCII only to match table's ascii column; truncate to length
    try:
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return s[:15]

def _username_str(v: object) -> str:
    # keep up to 255 chars; allow utf8 (table is utf8mb4)
    s = "" if v is None else str(v).strip()
    return s[:255]

def _month_year_from_ts(ts: int) -> int:
    return int(datetime.fromtimestamp(ts).strftime("%y%m"))

async def _bulk_insert_values_with_retry(
    table_name: str,
    colnames: tuple[str, ...],
    rows: list[tuple],
    ondup_sql: str,
    max_retries: int,
    op_name: str,
    batch_size: int = 2000,
) -> bool:
    """
    Insert rows using multi-VALUES and ON DUPLICATE KEY UPDATE with retries.
    - rows: list[tuple] aligned with colnames
    - ondup_sql: e.g. "ON DUPLICATE KEY UPDATE col = VALUES(col)"
    """
    if not rows:
        return True

    # Stable ordering reduces deadlocks dramatically (order by unique key columns first)
    rows = sorted(rows)

    placeholders_one = "(" + ",".join(["%s"] * len(colnames)) + ")"
    collist = ",".join(colnames)

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        values_clause = ",".join([placeholders_one] * len(chunk))
        sql = f"INSERT INTO {table_name} ({collist}) VALUES {values_clause} {ondup_sql}"
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
                break
            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):  # deadlock or lock wait timeout
                    attempt += 1
                    backoff = min(2.0, 0.25 * attempt)
                    logger.warning(
                        f"⚠️ {op_name} {('deadlock' if code==1213 else 'lock timeout')} on rows {i}-{i+len(chunk)}. "
                        f"Retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ {op_name} failed: {e}", exc_info=True)
                return False
            except Exception as e:
                logger.error(f"❌ Unexpected {op_name} failure: {e}", exc_info=True)
                return False
        else:
            logger.error(f"❌ {op_name} exceeded retries on rows {i}-{i+len(chunk)}")
            return False

    logger.debug(f"✅ Inserted {len(rows)} rows for {op_name} (multi-VALUES).")
    return True


class PokemonSQLProcessor:
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
    async def bulk_upsert_aggregated_pokemon_iv_monthly_batch_v2(
        cls,
        data_batch: list,
        max_retries=6
    ) -> int:
        """
        Ultra-fast path using a TEMPORARY TABLE and two set-based UPSERTs.
        Steps:
          - Create TEMPORARY TABLE tmp_apim
          - Multi-VALUES INSERT all rows into tmp_apim
          - Upsert spawnpoints from tmp_apim (GROUP BY spawnpoint) — ONLY where coords exist
          - Upsert aggregated_pokemon_iv_monthly from tmp_apim (GROUP BY key, SUM(inc))
        Returns number of rows consumed from data_batch (not number of rows upserted).
        """
        if not data_batch:
            return 0

        # Prepare rows for tmp table; sanitize here
        rows = []
        rows_total = 0
        rows_without_coords = 0
        uniq_spawnpoints_total = set()
        uniq_spawnpoints_with_coords = set()

        for d in data_batch:
            try:
                sp_hex = d.get("spawnpoint")
                if not sp_hex:
                    continue
                sp = int(sp_hex, 16)

                # iv is already a bucket at this point (0,25,50,75,90,95,100)
                iv = int(d["iv"])
                if iv not in (0, 25, 50, 75, 90, 95, 100):
                    continue

                month_year = int(datetime.fromtimestamp(int(d["first_seen"])).strftime("%y%m"))
                pokemon_id = int(d["pokemon_id"])
                form = str(d.get("form", 0))
                area_id = int(d["area_id"])
                inc = int(d.get("increment", 1))

                lat = d.get("latitude")
                lon = d.get("longitude")
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None

                rows.append((sp, lat, lon, pokemon_id, form, iv, area_id, month_year, inc))
                rows_total += 1
                uniq_spawnpoints_total.add(sp)
                if lat is None or lon is None:
                    rows_without_coords += 1
                else:
                    uniq_spawnpoints_with_coords.add(sp)
            except Exception:
                # skip malformed
                continue

        if not rows:
            return 0

        BATCH = 5000  # Chunk size for multi-values into temp table

        attempt = 0
        while attempt < max_retries:
            try:
                async with transaction(
                    isolation="READ COMMITTED",
                    lock_wait_timeout=10,
                ) as cur:
                    # 1) create temp table
                    await cur.execute("""
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_apim (
                            spawnpoint BIGINT NOT NULL,
                            latitude   DOUBLE NULL,
                            longitude  DOUBLE NULL,
                            pokemon_id SMALLINT NOT NULL,
                            form       VARCHAR(15) NOT NULL,
                            iv         SMALLINT NOT NULL,
                            area_id    SMALLINT NOT NULL,
                            month_year SMALLINT NOT NULL,
                            inc        INT NOT NULL,
                            INDEX idx_apim_sp (spawnpoint),
                            INDEX idx_apim_my (month_year),
                            INDEX idx_apim_key (spawnpoint, pokemon_id, form, iv, area_id, month_year)
                        ) ENGINE=InnoDB
                    """)

                    # 2) bulk insert into temp
                    placeholders = "(" + ",".join(["%s"] * 9) + ")"
                    for i in range(0, len(rows), BATCH):
                        chunk = rows[i:i+BATCH]
                        values_clause = ",".join([placeholders] * len(chunk))
                        flat = tuple(v for row in chunk for v in row)
                        await cur.execute(f"INSERT INTO tmp_apim VALUES {values_clause}", flat)

                    # 3) spawnpoints upsert (split into NEW vs UPDATED)
                    # 3a) Insert only brand-new spawnpoints (IGNORE avoids errors & only counts new)
                    await cur.execute("""
                        INSERT IGNORE INTO spawnpoints (spawnpoint, latitude, longitude)
                        SELECT
                        s.spawnpoint,
                        s.latitude,
                        s.longitude
                        FROM (
                        SELECT
                            t.spawnpoint,
                            ANY_VALUE(t.latitude)  AS latitude,
                            ANY_VALUE(t.longitude) AS longitude
                        FROM tmp_apim t
                        WHERE t.latitude IS NOT NULL AND t.longitude IS NOT NULL
                        GROUP BY t.spawnpoint
                        ) AS s
                    """)
                    new_spawnpoints = cur.rowcount  # pure count of newly inserted rows

                    # 3b) Update coords for existing rows when we have coords in this batch
                    #     rowcount here is "rows actually changed" (i.e., value differed)
                    await cur.execute("""
                        UPDATE spawnpoints sp
                        JOIN (
                        SELECT
                            t.spawnpoint,
                            ANY_VALUE(t.latitude)  AS latitude,
                            ANY_VALUE(t.longitude) AS longitude
                        FROM tmp_apim t
                        WHERE t.latitude IS NOT NULL AND t.longitude IS NOT NULL
                        GROUP BY t.spawnpoint
                        ) x  ON x.spawnpoint = sp.spawnpoint
                        SET sp.latitude  = x.latitude,
                            sp.longitude = x.longitude
                        WHERE
                            (sp.latitude  IS NULL OR sp.longitude IS NULL)  -- fill missing
                            OR (sp.latitude  <> x.latitude OR sp.longitude <> x.longitude) -- or changed
                    """)
                    updated_spawnpoints = cur.rowcount  # rows whose coords were actually updated

                    logger.info(
                        f"🧮 IV batch stats | rows_total={rows_total}, "
                        f"rows_without_coords={rows_without_coords}, "
                        f"uniq_spawnpoints_total={len(uniq_spawnpoints_total)}, "
                        f"uniq_spawnpoints_with_coords={len(uniq_spawnpoints_with_coords)}, "
                        f"new_spawnpoints={new_spawnpoints}, "
                        f"updated_spawnpoints={updated_spawnpoints}"
                    )
                    # 4) aggregated upsert
                    await cur.execute("""
                        INSERT INTO aggregated_pokemon_iv_monthly
                            (spawnpoint, pokemon_id, form, iv, area_id, month_year, total_count)
                        SELECT
                            t.spawnpoint,
                            t.pokemon_id,
                            t.form,
                            t.iv,
                            t.area_id,
                            t.month_year,
                            SUM(t.inc) AS total_count
                        FROM tmp_apim t
                        GROUP BY
                            t.spawnpoint, t.pokemon_id, t.form, t.iv, t.area_id, t.month_year
                        ON DUPLICATE KEY UPDATE
                            total_count = total_count + VALUES(total_count)
                    """)

                    # 5) cleanup temp + commit
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_apim")

                return len(rows)

            except aiomysql.Error as e:
                attempt += 1
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    backoff = min(2.0, 0.25 * attempt) + random.random() * 0.05
                    logger.warning(
                        f"⚠️ APIM temp-bulk upsert {('deadlock' if code==1213 else 'timeout')} "
                        f"retry {attempt}/{max_retries} in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ APIM temp-bulk upsert failed: {e}", exc_info=True)
                return 0
            except Exception as e:
                logger.error(f"❌ APIM temp-bulk upsert unexpected error: {e}", exc_info=True)
                return 0

        logger.error("❌ APIM temp-bulk upsert max retries reached")
        return 0


    # Currently unused; keeping for reference
    @classmethod
    async def bulk_upsert_aggregated_pokemon_iv_monthly_batch(
        cls,
        data_batch: list,
        max_retries=10
    ) -> int:
        """
        Batch upsert aggregated IV data using multi-VALUES with ON DUPLICATE KEY UPDATE.
        Unique key: (spawnpoint, pokemon_id, form, iv, area_id, month_year)
        """
        # Prepare lists for spawnpoint upsert and aggregated data.
        spawnpoints = {}       # sp_int -> (lat or None, lon or None)
        aggregated_values = [] # tuples for agg table


        for data in data_batch:
            try:
                # Spawnpoint: hex string -> int
                sp_hex = data.get('spawnpoint')
                if not sp_hex:
                    logger.warning("⚠️ Spawnpoint is None/empty; skipping record.")
                    continue
                try:
                    sp = int(sp_hex, 16)
                except Exception:
                    logger.warning(f"⚠️ Invalid spawnpoint hex value '{sp_hex}'; skipping record.")
                    continue

                # Coords: may be None; do NOT drop record if coords missing
                lat = None
                lon = None
                try:
                    if data.get('latitude') is not None:
                        lat = float(data['latitude'])
                    if data.get('longitude') is not None:
                        lon = float(data['longitude'])
                except (ValueError, TypeError):
                    lat, lon = None, None

                # Save first-seen coords per batch (do not care about duplicates here)
                if sp not in spawnpoints:
                    spawnpoints[sp] = (lat, lon)
                else:
                    # Prefer the first non-null coords we saw
                    cur_lat, cur_lon = spawnpoints[sp]
                    if cur_lat is None and lat is not None:
                        cur_lat = lat
                    if cur_lon is None and lon is not None:
                        cur_lon = lon
                    spawnpoints[sp] = (cur_lat, cur_lon)

                # IV bucket: trust Redis bucket, validate against VALID_IV_BUCKETS
                try:
                    bucket_iv = int(data['iv'])
                except Exception:
                    logger.warning("⚠️ Non-integer IV bucket; skipping record.")
                    continue
                if bucket_iv not in VALID_IV_BUCKETS:
                    logger.warning(f"⚠️ Invalid IV bucket {bucket_iv}; skipping record.")
                    continue

                # Month-year from first_seen
                try:
                    dt = datetime.fromtimestamp(int(data['first_seen']))
                    month_year = int(dt.strftime("%y%m"))
                except Exception:
                    logger.warning(f"⚠️ Invalid first_seen timestamp; skipping record.")
                    continue

                inc = int(data.get('increment', 1))

                aggregated_values.append((
                    sp,
                    int(data['pokemon_id']),
                    str(data.get('form', 0)),
                    bucket_iv,
                    int(data['area_id']),
                    month_year,
                    inc
                ))

            except Exception as e:
                logger.error(f"❌ Error processing data batch record: {e}", exc_info=True)

        if not aggregated_values:
            logger.warning("⚠️ No valid aggregated records to upsert.")
            return 0

        # --- Step 1: Spawnpoints upsert (only update coords if provided) ---
        sp_rows = []
        seen = set()
        for sp, (lat, lon) in spawnpoints.items():
            if sp in seen:
                continue
            seen.add(sp)
            sp_rows.append((sp, lat, lon))

        ondup_sp = """
        ON DUPLICATE KEY UPDATE
        latitude  = IF(VALUES(latitude)  IS NOT NULL, VALUES(latitude),  latitude),
        longitude = IF(VALUES(longitude) IS NOT NULL, VALUES(longitude), longitude)
        """
        ok = await _bulk_insert_values_with_retry(
            "spawnpoints",
            ("spawnpoint","latitude","longitude"),
            sp_rows,
            ondup_sp,
            max_retries,
            "spawnpoints upsert",
            batch_size=2000
        )
        if not ok:
            logger.error("❌ Failed to upsert spawnpoints after retries.")
            return 0

        # --- Step 2: Aggregated IV upsert ---
        ok = await _bulk_insert_values_with_retry(
            "aggregated_pokemon_iv_monthly",
            ("spawnpoint","pokemon_id","form","iv","area_id","month_year","total_count"),
            aggregated_values,
            "ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)",
            max_retries,
            "agg IV upsert",
            batch_size=2000
        )
        if not ok:
            logger.error("❌ Failed to upsert aggregated Pokémon IV data after retries.")
            return 0

        return len(aggregated_values)


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
