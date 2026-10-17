"""
Repair weekly counter totals from their daily counters.

Each weekly key (counter:pokemon_total:{area}:{monday}, and the raid, invasion,
quest and TTH equivalents) is written by the same pipeline as that week's
daily keys, so it should equal the sum of its seven days. Before the
crash-resilience fixes, a Redis wipe in the middle of a week lost everything
the still-open weekly key had counted before the wipe: the backups taken right
after overwrote its MySQL row with the partial count, while the days before
the wipe - already closed - kept their full rows. In those weeks the weekly
total equals only the days from the wipe onward.

This raises each field of a closed week to the sum of its daily fields when
that sum is larger. It never lowers a value, never touches a week that can
still be written, and leaves weeks alone whose dailies are incomplete (the sum
is then smaller, not larger). The wipe day itself was partial in both and
stays partial.

    python -m my_redis.utils.repair_weekly_totals             # dry run: what would change
    python -m my_redis.utils.repair_weekly_totals --list      # ...and list every affected week
    python -m my_redis.utils.repair_weekly_totals --apply     # write the repaired totals

With REDIS_MYSQL_BACKUPS on, --apply also marks the next backup as a full
one: incremental backups skip closed weeks, so only a full backup carries the
repaired values into MySQL.
"""
from __future__ import annotations

import argparse
import asyncio
import time
from collections import Counter
from datetime import datetime, timedelta

from my_redis.utils.mysql_backup import BACKUP_GRACE_S, area_offsets_from_redis, counter_last_write_utc

# weekly family -> its daily family
FAMILIES = {
    "pokemon_total": "pokemon_daily",
    "tth_pokemon": "tth_pokemon_daily",
    "raid_total": "raid_daily",
    "invasion": "invasion_daily",
    "quest": "quest_daily",
}
CHUNK = 100   # weeks per pipeline round (8 HGETALLs each)


def _daily_keys(weekly_key: str, daily_family: str) -> list[str] | None:
    parts = weekly_key.split(":")
    if len(parts) < 4 or len(parts[-1]) != 8:
        return None
    area = ":".join(parts[2:-1])
    try:
        monday = datetime.strptime(parts[-1], "%Y%m%d")
    except ValueError:
        return None
    return [f"counter:{daily_family}:{area}:{(monday + timedelta(days=d)).strftime('%Y%m%d')}" for d in range(7)]


def _as_int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


async def repair(client, families: dict[str, str] = FAMILIES, apply: bool = False, log=print) -> dict:
    now = time.time()
    offsets = await area_offsets_from_redis(client)   # areas without an offset: most cautious
    stats = {"weeks": 0, "open": 0, "damaged": 0, "fields_raised": 0, "added": 0}
    affected: list[tuple[str, int, int]] = []   # (key, weekly sum, daily sum)

    for weekly_family, daily_family in families.items():
        keys = [k if isinstance(k, str) else k.decode()
                async for k in client.scan_iter(match=f"counter:{weekly_family}:*", count=2000)]
        for i in range(0, len(keys), CHUNK):
            batch = []
            for key in keys[i:i + CHUNK]:
                days = _daily_keys(key, daily_family)
                if days is None:
                    continue
                stats["weeks"] += 1
                last = counter_last_write_utc(key, offsets)
                if last is None or last + BACKUP_GRACE_S >= now:
                    stats["open"] += 1
                    continue
                batch.append((key, days))
            if not batch:
                continue

            async with client.pipeline(transaction=False) as pipe:
                for key, days in batch:
                    pipe.hgetall(key)
                    for d in days:
                        pipe.hgetall(d)
                res = await pipe.execute()

            updates = []
            for n, (key, days) in enumerate(batch):
                weekly = {f: _as_int(v) for f, v in res[n * 8].items()}
                daily = Counter()
                for h in res[n * 8 + 1:(n + 1) * 8]:
                    daily.update({f: _as_int(v) for f, v in h.items()})
                raised = {f: v for f, v in daily.items() if v > weekly.get(f, 0)}
                if raised:
                    stats["damaged"] += 1
                    stats["fields_raised"] += len(raised)
                    stats["added"] += sum(v - weekly.get(f, 0) for f, v in raised.items())
                    affected.append((key, sum(weekly.values()), sum(daily.values())))
                    updates.append((key, raised))

            if apply and updates:
                async with client.pipeline(transaction=False) as pipe:
                    for key, raised in updates:
                        pipe.hset(key, mapping=raised)
                    await pipe.execute()

    stats["affected"] = affected
    verb = "repaired" if apply else "would repair"
    log(f"Weekly totals: {stats['weeks']} weeks checked ({stats['open']} still open, skipped); "
        f"{verb} {stats['damaged']} weeks, raising {stats['fields_raised']} fields by {stats['added']:,} in total")
    return stats


async def _main(apply: bool, list_weeks: bool) -> None:
    import config as AppConfig
    from my_redis.connect_redis import RedisManager

    manager = RedisManager()
    await manager.init_redis()
    client = await manager.check_redis_connection()
    if not client:
        raise SystemExit("Redis not reachable")
    stats = await repair(client, apply=apply)
    if list_weeks:
        for key, w, d in sorted(stats["affected"]):
            print(f"  {key}: weekly {w:,} -> sum of days {d:,}")
    await manager.close_redis()

    if apply and stats["damaged"] and AppConfig.redis_mysql_backups:
        import sql.connect_db as db
        from my_redis.utils.mysql_backup import META_LAST_FULL, set_meta
        try:
            await db.init_db()
            await set_meta(META_LAST_FULL, None)
            print("The next Redis backup will be a full one, so MySQL gets the repaired weeks too.")
        except Exception as e:
            print(f"Could not mark the next backup as full ({e}). Restart PsyduckV2 or wait for the "
                  f"daily full backup - until then MySQL still holds the old weekly totals.")
        finally:
            await db.close_db()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write the repaired totals (default: dry run)")
    ap.add_argument("--list", action="store_true", help="list every affected week")
    a = ap.parse_args()
    asyncio.run(_main(a.apply, a.list))
