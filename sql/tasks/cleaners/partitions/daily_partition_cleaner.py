from __future__ import annotations
from datetime import date, timedelta
from typing import Dict, List
from sql.connect_db import transaction
from utils.logger import logger


def _parse_upper_bound_as_date(desc: str | None) -> date | None:
    """
    PARTITION_DESCRIPTION for your daily partitions is 'YYYY-MM-DD' (the *upper* bound).
    Return a date or None if unparsable/sentinel.
    """
    if not desc or desc.upper() == "MAXVALUE":
        return None

    s = str(desc).strip()
    # information_schema.PARTITION_DESCRIPTION returns "'YYYY-MM-DD'" for RANGE COLUMNS(DATE)
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]

    try:
        # MySQL returns 'YYYY-MM-DD'
        return date.fromisoformat(s)
    except Exception:
        return None


async def clean_daily_partitions(
    table: str,
    column: str = "day_date",
    keep_days: int = 15,
    dry_run: bool = False,
) -> Dict[str, List[str] | int | str]:
    """
    Drops *whole day partitions* strictly older than your keep window.

    Semantics:
      - We KEEP the last `keep_days` days *inclusive of today*.
      - We DROP partitions whose upper bound (which is D+1) is <= `keep_from` (start of keep window).

    Example:
      today = 2025-09-26, keep_days=15 → keep_from = 2025-09-12
      Day partition 2025-09-11 has upper bound 2025-09-12 → dropped (<= keep_from).
      Day partition 2025-09-12 has upper bound 2025-09-13 → kept (> keep_from).

    Returns: { "dropped": [...], "kept": [...], "dry_run": 0/1, "error": "..."? }
    """
    keep_days = max(3, int(keep_days))
    keep_from = date.today() - timedelta(days=keep_days - 1)

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
                logger.warning(f"⚠️ daily-clean: table `{table}` not found; skipping.")
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
                logger.info(f"ℹ️ daily-clean: `{table}` is not partitioned; nothing to do.")
                return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run)}

            # Decide which partitions to drop
            to_drop: List[str] = []
            for name, desc in rows:
                if not name or name == "pMAX":
                    kept.append(name or "NULL")
                    continue
                ub = _parse_upper_bound_as_date(desc)
                if ub is None:
                    kept.append(name)
                    continue
                if ub <= keep_from:
                    to_drop.append(name)
                else:
                    kept.append(name)

            if not to_drop:
                logger.info(f"👌 daily-clean: nothing to drop for `{table}` (keep_days={keep_days}).")
                return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run)}

            if dry_run:
                logger.warning(f"🧹 [DRY-RUN] daily-clean `{table}` would drop: {to_drop}")
                return {"dropped": to_drop, "kept": kept, "dry_run": 1}

            # Drop in one ALTER when possible
            part_list = ", ".join(f"`{p}`" for p in to_drop)
            sql = f"ALTER TABLE `{table}` DROP PARTITION {part_list}"
            logger.warning(f"🧹 daily-clean `{table}` dropping {len(to_drop)} partition(s): {to_drop}")
            await cur.execute(sql)
            dropped.extend(to_drop)

    except Exception as e:
        logger.error(f"❌ daily-clean failed for `{table}`: {e}", exc_info=True)
        return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run), "error": repr(e)}

    logger.success(f"✅ daily-clean `{table}`: dropped={len(dropped)} kept={len(kept)}")
    return {"dropped": dropped, "kept": kept, "dry_run": int(dry_run)}
