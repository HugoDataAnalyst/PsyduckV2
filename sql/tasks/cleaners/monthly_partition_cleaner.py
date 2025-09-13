from __future__ import annotations
from datetime import date
from typing import Dict, List, Tuple
from sql.connect_db import transaction
from utils.logger import logger


def _first_of_month(d: date) -> date:
    return d.replace(day=1)


def _add_months(d: date, months: int) -> date:
    # Simple month arithmetic
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def _yymm(d: date) -> int:
    return int(d.strftime("%y%m"))  # YYMM


def _parse_upper_as_int(desc: str | None) -> int | None:
    """
    For monthly partitions the PARTITION_DESCRIPTION is numeric YYMM_NEXT.
    e.g., p2509 (Aug 2025) has upper bound 2509 if it was Aug? (We set VALUES LESS THAN (YYMM_NEXT))
    We rely on your ensure_monthly_partitions that uses that scheme.
    """
    if not desc or desc.upper() == "MAXVALUE":
        return None
    try:
        return int(str(desc))
    except Exception:
        return None


async def clean_monthly_partitions(
    table: str,
    column: str = "month_year",
    keep_months: int = 3,
    dry_run: bool = False,
) -> Dict[str, List[str] | int | str]:
    """
    Drops *whole month partitions* strictly older than your keep window.

    Semantics:
      - We KEEP the last `keep_months` months *including the current month*.
      - Let keep_from_month = first day of (current month - (keep_months-1)).
      - We DROP any partition whose PARTITION_DESCRIPTION (YYMM_NEXT) <= YYMM(keep_from_month).

    Example:
      today=2025-09-26, keep_months=3 → keep_from_month=2025-07-01 → cut=2507
      A partition for 2025-07 has upper=2508  -> 2508 <= 2507 ? No (kept).
      A partition for 2025-06 has upper=2507  -> 2507 <= 2507 Yes (drop).
    """
    keep_months = max(3, int(keep_months))
    today = date.today()
    keep_from_month = _add_months(_first_of_month(today), -(keep_months - 1))
    cutoff_yymm = _yymm(keep_from_month)

    dropped: List[str] = []
    kept: List[str] = []

    try:
        async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
            # Verify table
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (table,),
            )
            ct = await cur.fetchone()
            if not ct or int(ct[0]) == 0:
                logger.warning(f"⚠️ monthly-clean: table `{table}` not found; skipping.")
                return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run), "error": "table_not_found"}

            # List partitions
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
            if not rows:
                logger.info(f"ℹ️ monthly-clean: `{table}` is not partitioned; nothing to do.")
                return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run)}

            # Decide which partitions to drop
            to_drop: List[str] = []
            for name, desc in rows:
                if not name or name == "pMAX":
                    kept.append(name or "NULL")
                    continue
                ub = _parse_upper_as_int(desc)  # YYMM_NEXT (int)
                if ub is None:
                    kept.append(name)
                    continue
                if ub <= cutoff_yymm:
                    to_drop.append(name)
                else:
                    kept.append(name)

            if not to_drop:
                logger.info(f"👌 monthly-clean: nothing to drop for `{table}` (keep_months={keep_months}).")
                return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run)}

            if dry_run:
                logger.warning(f"🧹 [DRY-RUN] monthly-clean `{table}` would drop: {to_drop}")
                return {"dropped": to_drop, "kept": kept, "dry_run": 1}

            # Drop in one ALTER when possible
            part_list = ", ".join(f"`{p}`" for p in to_drop)
            sql = f"ALTER TABLE `{table}` DROP PARTITION {part_list}"
            logger.warning(f"🧹 monthly-clean `{table}` dropping {len(to_drop)} partition(s): {to_drop}")
            await cur.execute(sql)
            dropped.extend(to_drop)

    except Exception as e:
        logger.error(f"❌ monthly-clean failed for `{table}`: {e}", exc_info=True)
        return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run), "error": repr(e)}

    logger.success(f"✅ monthly-clean `{table}`: dropped={len(dropped)} kept={len(kept)}")
    return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run)}
