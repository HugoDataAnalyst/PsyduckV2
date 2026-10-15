"""
Scenario: a slow Redis must not turn into unbounded memory growth (Oct 14).

Golbat-like feed (one POST/s, no client timeout, no retry) while Redis is kept
busy by long Lua scripts - the shape of the production incident, where ingest
fell behind the arrival rate and every in-flight batch stayed in memory.

  0-20s     normal traffic
  20-110s   Redis hogged: ingest capacity < arrival rate, a backlog builds
  110-170s  normal traffic again; the backlog must drain

PASS when
  * the app sheds load with HTTP 503 once its in-flight cap is reached,
  * process memory stays bounded (growth < LIMIT_MB over the pre-hog level),
  * the backlog drains within 90s after Redis recovers.

  python test/scenario_backpressure.py [--rate 800] [--hog-seconds 90]
"""
import argparse
import asyncio
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import harness as h  # noqa: E402
from feeder import Feeder  # noqa: E402

LIMIT_MB = 250


async def main(rate: float, hog_seconds: int, hog_threads: int, hog_ms: int) -> int:
    h.banner(f"Backpressure: {rate:.0f} ev/s, Redis hogged for {hog_seconds}s "
             f"({hog_threads} x {hog_ms}ms Lua)")
    sampler = h.MemorySampler(every=2.0)
    sampler.start()
    f = Feeder(h.WEBHOOK_URL, rate=rate, mode="realistic", seed=99)
    inflight_series = []

    def tick(n, fd):
        inflight_series.append((n, len(fd.pending), dict(fd.stats.status)))
        if n % 10 == 0:
            rss = sampler.samples[-1][1] if sampler.samples else None
            print(f"[{n:4d}s] in-flight POSTs={len(fd.pending):4d}  rss={rss}MB  status={dict(fd.stats.status)}",
                  flush=True)

    await f.run(20, on_tick=tick)
    pre_hog = max(v for _, v in sampler.samples[-5:]) if sampler.samples else None
    print(f"pre-hog RSS: {pre_hog} MB  -> hogging Redis ...", flush=True)

    with h.RedisHog(threads=hog_threads, script_ms=hog_ms) as hog:
        await f.run(hog_seconds, on_tick=lambda n, fd: tick(20 + n, fd))
    hog_peak_posts = max(p for _, p, _ in inflight_series)
    print(f"Redis released after {hog.calls} hog scripts; in-flight POSTs peaked at {hog_peak_posts}", flush=True)

    released_at = time.time()
    await f.run(60, on_tick=lambda n, fd: tick(20 + hog_seconds + n, fd))

    # how long until the backlog is gone (no POST older than a couple of seconds)
    recovered_s = None
    while time.time() - released_at < 240:
        if len(f.pending) <= 2:
            recovered_s = round(time.time() - released_at, 1)
            break
        await asyncio.sleep(1)
    await f.drain(timeout=60)
    sampler.stop()
    await f.close()

    s = f.summary()
    peak = sampler.peak
    growth = round(peak - pre_hog, 1) if (peak and pre_hog) else None
    shed = sum(v for k, v in s["status"].items() if k == "503")
    result = {
        "rate": rate, "hog_seconds": hog_seconds,
        "feeder": s,
        "rss_pre_hog_mb": pre_hog, "rss_peak_mb": peak, "rss_growth_mb": growth,
        "in_flight_posts_peak": hog_peak_posts,
        "posts_rejected_503": shed,
        "backlog_drained_after_s": recovered_s,
        "rss_series": sampler.samples,
        "checks": {
            "sheds_load_with_503": shed > 0,
            f"rss_growth_below_{LIMIT_MB}MB": growth is not None and growth < LIMIT_MB,
            "drains_within_90s": recovered_s is not None and recovered_s <= 90,
        },
    }
    result["pass"] = all(result["checks"].values())
    path = h.write_result("backpressure", result)

    print(f"\nstatus codes      : {s['status']}")
    print(f"in-flight POSTs   : peak {hog_peak_posts}")
    print(f"RSS               : pre-hog {pre_hog} MB -> peak {peak} MB (growth {growth} MB)")
    print(f"backlog drained   : {recovered_s}s after Redis recovered")
    print(f"POST latency      : {s['latency_s']}")
    for k, v in result["checks"].items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"results -> {path}")
    print("SCENARIO:", "PASS" if result["pass"] else "FAIL")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=800)
    ap.add_argument("--hog-seconds", type=int, default=90)
    ap.add_argument("--hog-threads", type=int, default=6)
    ap.add_argument("--hog-ms", type=int, default=50)
    a = ap.parse_args()
    sys.exit(asyncio.run(main(a.rate, a.hog_seconds, a.hog_threads, a.hog_ms)))
