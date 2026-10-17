"""
reencode_hashes must shrink closed hashtable counters to listpack without
changing a single value, and leave keys that are still being written alone.

Seeds Redis db 3 (test stack) with prod-shaped pokemon_daily / pokemon_total
hashes (~800-2600 fields) under hash-max-listpack-entries 512, raises the
limit to 4096, dry-runs, applies, and compares every hash before/after.

  python test/check_reencode_hashes.py
"""
import asyncio
import os
import random
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.abspath(__file__)))   # no repo .env
os.environ.setdefault("LOG_FILE", "False")

import redis.asyncio as aredis  # noqa: E402
import time  # noqa: E402
from my_redis.utils.mysql_backup import BACKUP_GRACE_S, counter_last_write_utc  # noqa: E402
from my_redis.utils.reencode_hashes import reencode  # noqa: E402

METRICS = ["total", "iv100", "iv0", "pvp_little", "pvp_great", "pvp_ultra", "shiny"]


def fields(rng, n):
    out = {}
    while len(out) < n:
        pid = rng.randint(1, 1000)
        form = rng.choice([0, 0, 0, rng.randint(1, 3000)])
        out[f"{pid}:{form}:{rng.choice(METRICS)}"] = rng.randint(1, 5000)
    return out


async def main() -> int:
    r = aredis.Redis(host="127.0.0.1", port=16379, password="testpass", db=3, decode_responses=True)
    orig = (await r.config_get("hash-max-listpack-entries"))["hash-max-listpack-entries"]
    rng = random.Random(4)
    await r.flushdb()
    await r.config_set("hash-max-listpack-entries", 512)
    today = datetime.now(timezone.utc)
    seeded = {}
    for area in ("A1", "A2", "A3"):
        for d in range(0, 40):                                   # d=0 is today: still written
            k = f"counter:pokemon_daily:{area}:{(today - timedelta(days=d)).strftime('%Y%m%d')}"
            seeded[k] = fields(rng, rng.randint(800, 1700))
        for w in range(0, 6):
            monday = today - timedelta(days=today.weekday(), weeks=w)
            k = f"counter:pokemon_total:{area}:{monday.strftime('%Y%m%d')}"
            seeded[k] = fields(rng, rng.randint(1300, 2600))
    for k, v in seeded.items():
        await r.hset(k, mapping=v)
    # No geofences in this db, so the tool treats every area as unknown-offset
    # (most cautious); "open" is whatever that rule says - today's keys always,
    # yesterday's too until ~16:00 UTC.
    this_monday = (today - timedelta(days=today.weekday())).strftime("%Y%m%d")
    open_keys = [k for k in seeded if counter_last_write_utc(k, {}) + BACKUP_GRACE_S >= time.time()]
    must_be_open = [k for k in seeded
                    if (":pokemon_daily:" in k and k.endswith(today.strftime("%Y%m%d")))
                    or (":pokemon_total:" in k and k.endswith(this_monday))]
    assert set(must_be_open) <= set(open_keys)

    await r.config_set("hash-max-listpack-entries", 4096)
    dry = await reencode(r, apply=False)
    applied = await reencode(r, apply=True)

    changed = 0
    for k, v in seeded.items():
        now = await r.hgetall(k)
        if now != {f: str(x) for f, x in v.items()}:
            changed += 1
    encodings = {k: await r.object("encoding", k) for k in seeded}
    closed_compact = all(encodings[k] == "listpack" for k in seeded if k not in open_keys)
    open_untouched = all(encodings[k] == "hashtable" for k in open_keys)

    await r.flushdb()
    await r.config_set("hash-max-listpack-entries", orig)
    await r.aclose()
    print(f"values changed: {changed}; closed keys now listpack: {closed_compact}; "
          f"open keys left as hashtable: {open_untouched} ({len(open_keys)})")
    ok = (changed == 0 and closed_compact and open_untouched and dry["converted"] == applied["converted"]
          and applied["used_memory_mb_after"] < applied["used_memory_mb_before"])
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
