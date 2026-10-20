"""
Ingest cost benchmark: what one webhook event costs the app and Redis.

Feeds a fixed rate of valid, unique events (exact mode) for a while and reports
  * Redis commands per event, overall and for the per-event extras
    (PING, LLEN, HLEN) that cost a network round trip each
  * app CPU seconds per 1,000 events (utime+stime of the app's PID 1)
  * POST latency

Run it against an image, rebuild with the change, run it again, compare.

  python test/bench_ingest.py [--rate 1500] [--seconds 30] [--label before]
"""
import argparse
import asyncio
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as h  # noqa: E402
from feeder import Feeder  # noqa: E402

WATCH = ("ping", "llen", "hlen", "rpush", "hsetnx", "hincrby", "zadd", "exec", "multi")


def app_cpu_seconds() -> float:
    out = subprocess.run(["docker", "exec", h.APP_CONTAINER, "cat", "/proc/1/stat"],
                         capture_output=True, text=True).stdout.split()
    ticks = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
    return (int(out[13]) + int(out[14])) / ticks


def redis_stats() -> dict:
    r = h.rds()
    cs = r.info("commandstats")
    stats = {c: int(cs.get(f"cmdstat_{c}", {}).get("calls", 0)) for c in WATCH}
    stats["_total"] = int(r.info("stats")["total_commands_processed"])
    stats["_usec"] = sum(int(v.get("usec", 0)) for v in cs.values())
    return stats


async def main(rate: float, seconds: int, label: str, mode: str = "exact") -> int:
    h.banner(f"Ingest benchmark [{label}] - {rate:.0f} ev/s ({mode}) for {seconds}s")
    s0, c0 = redis_stats(), app_cpu_seconds()
    f = Feeder(h.WEBHOOK_URL, rate=rate, mode=mode, seed=99, timeout=None)
    t0 = time.time()
    await f.run(seconds)
    await f.drain(120)
    await f.close()
    wall = time.time() - t0
    time.sleep(1)
    s1, c1 = redis_stats(), app_cpu_seconds()
    summ = f.summary()
    events = summ["events_accepted"] or 1
    pokemon = summ["expected"]["iv_rows"] or 1

    per_event = {k: (s1[k] - s0[k]) / events for k in WATCH}
    result = {
        "label": label, "rate": rate, "mode": mode, "seconds": seconds, "events": events, "wall_s": round(wall, 1),
        "status": summ["status"],
        "redis_cmds_per_event": round((s1["_total"] - s0["_total"]) / events, 2),
        "redis_usec_per_event": round((s1["_usec"] - s0["_usec"]) / events, 2),
        "per_event": {k: round(v, 3) for k, v in per_event.items() if v},
        "roundtrip_extras_per_pokemon": round(sum(s1[k] - s0[k] for k in ("ping", "llen", "hlen")) / pokemon, 2),
        "app_cpu_s_per_1000_events": round((c1 - c0) / events * 1000, 3),
        "post_latency_s": summ["latency_s"],
    }
    for k, v in result.items():
        print(f"{k:30}: {v}")
    path = h.write_result(f"bench_ingest_{label}", result)
    print(f"results -> {path}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=1500)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--label", default="run")
    ap.add_argument("--mode", default="exact", choices=["exact", "realistic"],
                    help="exact: every event valid and unique; realistic: production seen_type mix + re-sends")
    a = ap.parse_args()
    sys.exit(asyncio.run(main(a.rate, a.seconds, a.label, a.mode)))
