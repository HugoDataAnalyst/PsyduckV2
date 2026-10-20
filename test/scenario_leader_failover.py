"""
Scenario: after a crash, a quick restart must not leave the app without a leader.

The leader lock (psyduckv2:leader:main, 30s TTL) outlives a killed process.
SIGKILL the app (crash / OOM kill), start it again immediately while the dead
leader's lock is still in Redis, then feed traffic.

PASS when the restarted worker takes leadership once the stale lock expires
and its background services run - proven by fed events reaching MariaDB
(only the leader's flushers write them).

  python test/scenario_leader_failover.py
"""
import asyncio
import subprocess
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import harness as h  # noqa: E402
from feeder import Feeder  # noqa: E402

LOCK = "psyduckv2:leader:main"


async def main() -> int:
    h.banner("Leader failover - immediate restart after SIGKILL")
    r = h.rds()
    subprocess.run(["docker", "kill", h.APP_CONTAINER], check=True, capture_output=True)
    ttl = r.ttl(LOCK)
    print(f"app killed; stale leader lock still present with TTL {ttl}s - restarting immediately")
    since = h._now_iso()
    h.start("app")
    h.wait_app_ready(since)

    before = h.db_counts()
    f = Feeder(h.WEBHOOK_URL, rate=100, mode="exact", seed=5)
    await f.run(10)
    await f.drain(60)
    await f.close()
    exp = f.summary()["expected"]
    print(f"fed {f.stats.events_accepted} events; waiting for them to reach MariaDB ...")

    t0 = time.time()
    landed = {}
    while time.time() - t0 < 120:
        landed = {k: h.db_counts()[k] - before[k] for k in exp}
        if landed == exp:
            break
        time.sleep(5)
    took = round(time.time() - t0, 1)
    logs = h.app_logs(since)
    result = {
        "stale_lock_ttl_at_restart_s": ttl,
        "expected": exp, "landed": landed,
        "seconds_until_landed": took if landed == exp else None,
        "became_leader": ("[LEADER]" in logs) or ("took over leadership" in logs),
        "buffers_now": h.buffer_lengths(),
        "pass": landed == exp,
    }
    print(f"expected {exp}\nlanded   {landed}")
    print(f"became leader: {result['became_leader']}   buffers: {result['buffers_now']}")
    path = h.write_result("leader_failover", result)
    print(f"results -> {path}")
    print("SCENARIO:", "PASS" if result["pass"] else "FAIL")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
