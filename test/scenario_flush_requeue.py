"""
Scenario: buffered SQL batches must survive a database outage and a crash.

Part A - DB outage
  1. stop MariaDB
  2. feed exact-mode traffic while every flush attempt fails
  3. start MariaDB again and wait for the flushers to catch up
  PASS when every table holds exactly what the feeder delivered
  (no rows lost, and no shiny counts doubled by a retry).

Part B - batch orphaned by a crash mid-flush
  A flush that dies between RENAME and DELETE (e.g. OOM kill) leaves its
  batch under the temporary key. Plant one under the legacy fixed name the
  current code uses, restart the app, and check it is recovered.
  PASS when the orphan is merged back and its rows reach MariaDB.

  python test/scenario_flush_requeue.py
"""
import asyncio
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import harness as h  # noqa: E402
from feeder import Feeder  # noqa: E402

KEYS = ("iv_rows", "shiny_total", "raids", "quests", "invasions")


def delta(after: dict, before: dict) -> dict:
    return {k: after[k] - before[k] for k in KEYS}


async def part_a() -> dict:
    h.banner("Part A - MariaDB outage while buffers flush")
    before = h.db_counts()
    print("baseline SQL counts:", before)

    h.stop("mariadb")
    print("MariaDB stopped; feeding for 20s while every flush fails ...")
    f = Feeder(h.WEBHOOK_URL, rate=100, mode="exact", seed=7)
    await f.run(20)
    await f.drain(60)
    await f.close()
    s = f.summary()
    print("feeder:", {k: s[k] for k in ("events_sent", "events_accepted", "status")})

    # let several flush cycles (5s) fail against the dead database
    time.sleep(20)
    buffers_during = h.buffer_lengths()
    print("Redis buffers while DB is down:", buffers_during)

    h.start("mariadb")
    print("MariaDB started; waiting for flushers to catch up ...")
    expected = s["expected"]
    got = {}
    for _ in range(36):  # up to 3 min
        time.sleep(5)
        try:
            got = delta(h.db_counts(), before)
        except Exception as e:  # DB still starting
            print("  (db not ready yet:", type(e).__name__, ")")
            continue
        if got == expected:
            break

    lost = {k: expected[k] - got.get(k, 0) for k in KEYS}
    result = {
        "expected": expected,
        "landed_in_sql": got,
        "difference_expected_minus_sql": lost,
        "buffers_while_db_down": buffers_during,
        "buffers_after": h.buffer_lengths(),
        "pass": got == expected,
    }
    print("expected :", expected)
    print("in SQL   :", got)
    print("lost (+) / duplicated (-):", lost)
    print("PART A:", "PASS" if result["pass"] else "FAIL")
    return result


def part_b() -> dict:
    h.banner("Part B - batch orphaned by a crash mid-flush (legacy fixed temp key)")
    r = h.rds()
    area_id = int(h.sql_val("SELECT MIN(id) FROM area_names"))
    now = int(time.time())
    n = 250
    tag = f"{now:x}"[-6:]
    lines, coords = [], {}
    for i in range(n):
        sp = f"{tag}{i:06x}"
        lines.append(f"{sp}|25|0|98|30|{area_id}|{now - i}")
        coords[sp] = "38.71,-9.24"

    before = int(h.sql_val("SELECT COUNT(*) FROM pokemon_iv_daily_events"))
    # exactly what the current code leaves behind if it dies after RENAME
    r.delete("buffer:pokemon_iv_events:flushing", "buffer:pokemon_iv_coords:flushing")
    r.rpush("buffer:pokemon_iv_events:flushing", *lines)
    r.hset("buffer:pokemon_iv_coords:flushing", mapping=coords)
    print(f"planted orphan with {n} IV events; restarting app ...")

    waited = h.restart_app()
    print(f"app ready again after {waited:.0f}s; waiting for flushes ...")
    landed = 0
    for _ in range(18):
        time.sleep(5)
        landed = int(h.sql_val("SELECT COUNT(*) FROM pokemon_iv_daily_events")) - before
        if landed >= n:
            break
    leftover = {k: r.llen(k) for k in r.scan_iter(match="buffer:pokemon_iv_events:*flushing*")}
    result = {"orphaned_events": n, "recovered_rows": landed, "orphan_keys_left": leftover,
              "pass": landed == n and not any(leftover.values())}
    print(f"orphaned={n} recovered_rows={landed} orphan_keys_left={leftover}")
    print("PART B:", "PASS" if result["pass"] else "FAIL")
    return result


async def main():
    results = {"part_a_db_outage": await part_a(), "part_b_crash_orphan": part_b()}
    path = h.write_result("flush_requeue", results)
    print(f"\nresults -> {path}")
    ok = all(v["pass"] for v in results.values())
    print("SCENARIO:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
