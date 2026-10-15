"""
Counter retrieval without KEYS must find exactly the keys KEYS found.

Seeds a separate Redis db (2) of the test stack with counter keys for every
family the API reads - several areas (one only in area_names, as for an area
removed from Koji), hourly/daily/monthly slots over 400 days, weather keys
with their 0/1 suffix - then, for each family x area x window, compares:

  old: KEYS pattern + filter_keys_by_time
  new: filtering_keys.find_time_keys (generated names + EXISTS, SCAN fallback)

plus the pipelined aggregate_keys against a one-HGETALL-per-key reference, and
the Redis time each path spends (INFO commandstats).

  python test/check_counter_retrieval.py
"""
import asyncio
import os
import random
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# config.py loads ./.env with override=True: run from test/ (no .env there) so the
# repo's own .env can never redirect these probes away from the test stack.
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.environ.update(REDIS_HOST="127.0.0.1", REDIS_SERVER_PORT="16379", REDIS_PASSWORD="testpass", REDIS_DB="2",
                  DB_HOST="127.0.0.1", DB_PORT="13306", DB_USER="psyduck", DB_PASSWORD="psyduck",
                  DB_NAME="PsyduckV2", LOG_FILE="False", LOG_LEVEL="WARNING")

import sql.connect_db as db  # noqa: E402
from my_redis.connect_redis import RedisManager  # noqa: E402
from my_redis.utils import filtering_keys as fk  # noqa: E402

REMOVED_AREA = "RemovedArea99"
FAMILIES = {
    # family: (time_format, suffixes)
    "pokemon_hourly": ("%Y%m%d%H", None), "pokemon_daily": ("%Y%m%d", None), "pokemon_total": ("%Y%m%d", None),
    "tth_pokemon_hourly": ("%Y%m%d%H", None), "tth_pokemon_daily": ("%Y%m%d", None), "tth_pokemon": ("%Y%m%d", None),
    "raid_hourly": ("%Y%m%d%H", None), "raid_daily": ("%Y%m%d", None), "raid_total": ("%Y%m%d", None),
    "invasion_hourly": ("%Y%m%d%H", None), "invasion_daily": ("%Y%m%d", None), "invasion": ("%Y%m%d", None),
    "quest_hourly": ("%Y%m%d%H", None), "quest_daily": ("%Y%m%d", None), "quest": ("%Y%m%d", None),
    "pokemon_weather_iv": ("%Y%m", ("0", "1")),
}


def _slots(fmt, now):
    if fmt == "%Y%m%d%H":
        return [(now - timedelta(hours=h)).strftime(fmt) for h in range(0, 24 * 75)]
    if fmt == "%Y%m%d":
        return [(now - timedelta(days=d)).strftime(fmt) for d in range(0, 400)]
    return sorted({(now - timedelta(days=d)).strftime(fmt) for d in range(0, 400)})


async def seed(client, areas, now):
    await client.flushdb()
    rng = random.Random(5)
    n = 0
    async with client.pipeline(transaction=False) as pipe:
        for fam, (fmt, sfx) in FAMILIES.items():
            for area in areas:
                for slot in _slots(fmt, now):
                    if rng.random() < 0.15:          # sparse, like real areas
                        continue
                    for tail in (sfx or (None,)):
                        key = f"counter:{fam}:{area}:{slot}" + (f":{tail}" if tail else "")
                        pipe.hset(key, mapping={f"{rng.randint(1, 30)}:0:total": rng.randint(1, 50),
                                                f"{rng.randint(1, 30)}:0:iv100": 1})
                        n += 1
        # unrelated keys KEYS has to wade through
        for i in range(30000):
            pipe.hset(f"ts:pokemon:total:{areas[i % len(areas)]}:{i}:0", "1", 1)
        await pipe.execute()
    return n


async def old_find(client, pattern, fmt, start, end, suffixes=None):
    keys = await client.keys(pattern)
    return fk.filter_keys_by_time(keys, fmt, start, end, component_index=-2 if suffixes else -1)


async def old_aggregate(client, keys, mode):
    out = {}
    for key in keys:
        data = await client.hgetall(key)
        if mode == "sum":
            for f, v in data.items():
                out[f] = out.get(f, 0) + int(v)
        else:
            out[key] = {k: int(v) for k, v in data.items()}
    return out


async def cmdstats(client, *cmds):
    info = await client.info("commandstats")
    return {c: (info.get(f"cmdstat_{c}", {}).get("calls", 0), info.get(f"cmdstat_{c}", {}).get("usec", 0)) for c in cmds}


def diff(a, b):
    return {c: (b[c][0] - a[c][0], b[c][1] - a[c][1]) for c in a}


async def main() -> int:
    await db.init_db()
    await db.execute("INSERT IGNORE INTO area_names (name) VALUES (%s)", (REMOVED_AREA,))
    rm = RedisManager()
    await rm.init_redis()
    client = await rm.check_redis_connection()

    areas = await fk.known_areas()
    assert REMOVED_AREA in areas, "area_names should list the removed area"
    now = datetime.utcnow()
    n = await seed(client, areas, now)
    print(f"seeded {n} counter keys + 30000 ts keys over {len(areas)} areas")

    windows = {
        "24h": (now - timedelta(hours=24), now),
        "7d unaligned": (now - timedelta(days=7, minutes=37), now - timedelta(minutes=5)),
        "all-time": (datetime(2025, 1, 1), now),
    }
    failures = 0
    checks = 0
    old_cost = {"keys": (0, 0)}
    new_cost = {"exists": (0, 0), "scan": (0, 0)}
    for limit in (fk.MAX_GENERATED_KEYS, 50):          # second pass forces the SCAN fallback
        fk.MAX_GENERATED_KEYS = limit
        for fam, (fmt, sfx) in FAMILIES.items():
            for area in ("global", areas[0], REMOVED_AREA):
                pattern = f"counter:{fam}:*" if area == "global" else f"counter:{fam}:{area}:*"
                for wname, (start, end) in windows.items():
                    s0 = await cmdstats(client, "keys")
                    old = await old_find(client, pattern, fmt, start, end, sfx)
                    s1 = await cmdstats(client, "keys", "exists", "scan")
                    new = await fk.find_time_keys(client, pattern, fmt, start, end, suffixes=sfx)
                    s2 = await cmdstats(client, "exists", "scan")
                    d_old, d_new = diff(s0, {"keys": s1["keys"]}), diff({k: s1[k] for k in ("exists", "scan")}, s2)
                    old_cost["keys"] = tuple(x + y for x, y in zip(old_cost["keys"], d_old["keys"]))
                    for c in new_cost:
                        new_cost[c] = tuple(x + y for x, y in zip(new_cost[c], d_new[c]))
                    checks += 1
                    if sorted(old) != sorted(new):
                        failures += 1
                        print(f"MISMATCH {fam} {area} {wname} limit={limit}: old {len(old)} new {len(new)}")
        fk.MAX_GENERATED_KEYS = 20000

    keys = await old_find(client, "counter:pokemon_daily:*", "%Y%m%d", windows["all-time"][0], now)
    for mode in ("sum", "grouped"):
        checks += 1
        if await fk.aggregate_keys(keys, mode) != await old_aggregate(client, keys, mode):
            failures += 1
            print(f"MISMATCH aggregate_keys mode={mode}")

    await client.flushdb()
    await db.execute("DELETE FROM area_names WHERE name = %s", (REMOVED_AREA,))
    await rm.close_redis()
    await db.close_db()

    print(f"key discovery, Redis time spent - old KEYS: {old_cost['keys'][0]} calls / {old_cost['keys'][1] / 1000:.1f} ms; "
          f"new: EXISTS {new_cost['exists'][0]} / {new_cost['exists'][1] / 1000:.1f} ms, "
          f"SCAN {new_cost['scan'][0]} / {new_cost['scan'][1] / 1000:.1f} ms")
    print(f"{checks - failures}/{checks} checks identical")
    print("RESULT:", "PASS" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
