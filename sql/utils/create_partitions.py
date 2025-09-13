from __future__ import annotations
from datetime import date, timedelta
from sql.connect_db import transaction
from utils.logger import logger

def _pname(d: date) -> str:
    return "p" + d.strftime("%Y%m%d")

def _upper_str(d: date) -> str:
    # VALUES LESS THAN (next day)
    return (d + timedelta(days=1)).strftime("%Y-%m-%d")

async def ensure_daily_partitions(
    table: str = "pokemon_iv_daily_events",
    column: str = "day_date",
    days_back: int = 2,
    days_forward: int = 35,
) -> dict:
    added, skipped = [], []
    days_back = max(0, int(days_back))
    days_forward = max(0, int(days_forward))

    try:
        async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
            # Who am I / where am I?
            await cur.execute("SELECT DATABASE()")
            dbrow = await cur.fetchone()
            dbname = dbrow[0] if dbrow else "(unknown)"
            logger.debug(f"🗄️ ensure_daily_partitions on `{dbname}`.`{table}` ({column})")

            # Check table exists
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (table,),
            )
            ct = await cur.fetchone()
            if not ct or ct[0] == 0:
                logger.warning(f"⚠️ Table `{table}` not found in `{dbname}`; skipping.")
                return {"added": added, "skipped": skipped, "error": "table_not_found"}

            # Grab existing partitions (name + description helps debugging)
            await cur.execute(
                """
                SELECT PARTITION_NAME, PARTITION_DESCRIPTION
                FROM information_schema.PARTITIONS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = %s
                  AND PARTITION_NAME IS NOT NULL
                ORDER BY PARTITION_DESCRIPTION
                """,
                (table,),
            )
            rows = await cur.fetchall() or []
            existing = {r[0] for r in rows}
            logger.debug(f"🔎 existing partitions ({len(rows)}): {rows}")

            # No partitions or pMAX, show CREATE TABLE for clarity
            if not rows or "pMAX" not in existing:
                await cur.execute(f"SHOW CREATE TABLE `{table}`")
                show = await cur.fetchone()
                logger.warning(
                    "⚠️ Table is not partitioned or missing pMAX. SHOW CREATE TABLE follows."
                )
                logger.warning(f"{show[1] if show and len(show) > 1 else show}")
                return {"added": added, "skipped": list(existing)}

            # Build targets (today +/- window)
            today = date.today()
            targets = []
            for delta in range(-days_back, days_forward + 1):
                d = today + timedelta(days=delta)
                pname = _pname(d)
                upper = _upper_str(d)
                targets.append((pname, upper))
            targets.sort(key=lambda t: t[1])  # ascending by upper bound

            # Create missing partitions by repeatedly splitting pMAX
            for pname, upper in targets:
                if pname in existing:
                    skipped.append(pname)
                    continue

                sql = (
                    f"ALTER TABLE `{table}` "
                    f"REORGANIZE PARTITION pMAX INTO ("
                    f"  PARTITION `{pname}` VALUES LESS THAN ('{upper}'),"
                    f"  PARTITION pMAX VALUES LESS THAN (MAXVALUE)"
                    f")"
                )
                try:
                    logger.debug(f"🧱 creating partition for {table} {pname} < '{upper}'")
                    await cur.execute(sql)
                    existing.add(pname)
                    added.append(pname)
                    logger.info(f"➕ Created partition `{pname}` for {table} (VALUES LESS THAN '{upper}')")
                except Exception as e:
                    # Log full exception info (class + args)
                    logger.error(
                        f"❌ Failed creating partition `{pname}`: "
                        f"{type(e).__name__} args={getattr(e, 'args', None)}",
                        exc_info=True,
                    )

    except Exception as e:
        logger.error(
            f"❌ ensure_daily_partitions failed for `{table}`: "
            f"{type(e).__name__} args={getattr(e, 'args', None)} repr={e!r}",
            exc_info=True,
        )
        return {"added": added, "skipped": skipped, "error": repr(e)}

    if added:
        logger.success(f"✅ Ensured daily partitions for `{table}` — added {len(added)}")
    else:
        logger.info(f"👌 No new daily partitions for `{table}`")
    return {"added": added, "skipped": skipped}
