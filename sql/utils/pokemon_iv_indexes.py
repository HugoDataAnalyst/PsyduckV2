"""
Secondary indexes of pokemon_iv_daily_events: target layout and how to get there.

The heatmap query filters one area and one day, optionally by pokemon_id/form,
iv and level, and groups by spawnpoint/pokemon_id/form. Measured with EXPLAIN
on a production-sized table:
  * ix_ev_area_day_sp_pkf (spawnpoint first) was never chosen - dead weight,
    about a third of the index space;
  * no index held `level`, so any level filter scanned the whole day
    partition through the primary key (~7M rows instead of ~1.5M index entries).

Target: the two useful indexes, each with `level` added so every heatmap
filter combination is answered from an index alone.

Used by the alembic migration (applied automatically only to small tables)
and by the manual tool for large ones:

    python -m sql.utils.pokemon_iv_indexes            # show current vs target
    python -m sql.utils.pokemon_iv_indexes --apply    # build new, then drop old

The new indexes are built online (ALGORITHM=INPLACE, LOCK=NONE: inserts keep
flowing) before the old ones are dropped, so queries stay fast throughout.
Needs free disk space for the new indexes while both sets exist.
"""
from __future__ import annotations

TABLE = "pokemon_iv_daily_events"

TARGET = {
    "ix_ev_area_day_pid_iv_lvl_sp_f": "(area_id, day_date, pokemon_id, iv, level, spawnpoint, form)",
    "ix_ev_area_day_iv_lvl_sp_pkf":   "(area_id, day_date, iv, level, spawnpoint, pokemon_id, form)",
}
OBSOLETE = ("ix_ev_area_day_sp_pkf", "ix_ev_area_day_iv_sp_pkf", "ix_ev_area_day_pid_iv_sp_f")

# Tables at or below this many rows are converted by the migration itself
# (seconds); larger ones are left to the manual tool, run off-peak.
AUTO_MAX_ROWS = 5_000_000

CURRENT_INDEXES_SQL = (
    "SELECT DISTINCT index_name FROM information_schema.statistics "
    "WHERE table_schema = DATABASE() AND table_name = %s AND index_name <> 'PRIMARY'"
)
ROW_ESTIMATE_SQL = (
    "SELECT COALESCE(SUM(table_rows), 0) FROM information_schema.partitions "
    "WHERE table_schema = DATABASE() AND table_name = %s"
)


def plan(existing: set[str]) -> list[str]:
    """ALTER statements to reach TARGET from `existing`: build first, drop after."""
    steps = []
    to_add = [f"ADD INDEX {name} {cols}" for name, cols in TARGET.items() if name not in existing]
    if to_add:
        steps.append(f"ALTER TABLE {TABLE} {', '.join(to_add)}, ALGORITHM=INPLACE, LOCK=NONE")
    to_drop = [f"DROP INDEX {name}" for name in OBSOLETE if name in existing]
    if to_drop:
        steps.append(f"ALTER TABLE {TABLE} {', '.join(to_drop)}, ALGORITHM=NOCOPY, LOCK=NONE")
    return steps


async def _main(apply: bool) -> None:
    import time
    import sql.connect_db as db

    await db.init_db()
    try:
        rows = await db.fetch_all(CURRENT_INDEXES_SQL, (TABLE,))
        existing = {r["index_name"] for r in rows}
        estimate = int(await db.fetch_val(ROW_ESTIMATE_SQL, (TABLE,)) or 0)
        steps = plan(existing)
        print(f"{TABLE}: ~{estimate:,} rows; current secondary indexes: {sorted(existing)}")
        if not steps:
            print("Already at the target layout - nothing to do.")
            return
        for s in steps:
            print(("RUN  " if apply else "PLAN ") + s)
        if not apply:
            print("Dry run. Re-run with --apply (off-peak; the build reads the whole table).")
            return
        for s in steps:
            t0 = time.monotonic()
            await db.execute(s)
            print(f"done in {time.monotonic() - t0:,.0f}s: {s[:80]}...")
    finally:
        await db.close_db()


if __name__ == "__main__":
    import argparse
    import asyncio

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="run the ALTERs (default: show the plan)")
    asyncio.run(_main(ap.parse_args().apply))
