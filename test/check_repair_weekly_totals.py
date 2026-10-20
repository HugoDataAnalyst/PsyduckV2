"""
repair_weekly_totals must restore exactly the counts a mid-week wipe took from
a weekly key, and touch nothing else.

Seeds Redis db 3 (test stack) with five weeks of daily + weekly keys for each
family: intact weeks, weeks "wiped" on a given day (weekly = days from the
wipe on), a week whose dailies are missing (must be left alone) and the
current, still-open week (must be left alone even if it looks damaged).

  python test/check_repair_weekly_totals.py
"""
import asyncio
import os
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.abspath(__file__)))   # no repo .env
os.environ.setdefault("LOG_FILE", "False")

import redis.asyncio as aredis  # noqa: E402
from my_redis.utils.repair_weekly_totals import FAMILIES, repair  # noqa: E402


async def main() -> int:
    r = aredis.Redis(host="127.0.0.1", port=16379, password="testpass", db=3, decode_responses=True)
    await r.flushdb()
    rng = random.Random(9)
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    this_monday = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    expected = {}          # weekly key -> hash it must hold afterwards
    kinds = Counter()
    for weekly_fam, daily_fam in FAMILIES.items():
        for area in ("Area1", "Area2"):
            for w in range(0, 5):
                monday = this_monday - timedelta(weeks=w)
                wk = f"counter:{weekly_fam}:{area}:{monday.strftime('%Y%m%d')}"
                days = [{f"{p}:0:total": rng.randint(1, 500) for p in rng.sample(range(1, 60), 20)} for _ in range(7)]
                kind = "open" if w == 0 else rng.choice(["intact", "wiped", "no_dailies"])
                wipe_day = rng.randint(1, 6)
                full = Counter()
                for d in days:
                    full.update(d)
                if kind in ("wiped", "open"):
                    weekly = Counter()
                    for d in days[wipe_day:]:
                        weekly.update(d)
                else:
                    weekly = full
                for di, d in enumerate(days):
                    if kind != "no_dailies":
                        await r.hset(f"counter:{daily_fam}:{area}:{(monday + timedelta(days=di)).strftime('%Y%m%d')}",
                                     mapping=d)
                await r.hset(wk, mapping=dict(weekly))
                expected[wk] = dict(full) if kind == "wiped" else dict(weekly)
                kinds[kind] += 1

    dry = await repair(r, apply=False)
    applied = await repair(r, apply=True)
    again = await repair(r, apply=False)

    wrong = 0
    for wk, want in expected.items():
        got = {f: int(v) for f, v in (await r.hgetall(wk)).items()}
        if got != want:
            wrong += 1
    await r.flushdb()
    await r.aclose()

    print(f"weeks seeded: {dict(kinds)}")
    print(f"weekly keys not as expected afterwards: {wrong}; second dry run finds {again['damaged']} to repair")
    ok = (wrong == 0 and dry["damaged"] == applied["damaged"] == kinds["wiped"] and again["damaged"] == 0)
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
