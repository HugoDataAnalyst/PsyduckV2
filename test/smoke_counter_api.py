"""
Smoke test: every counter endpoint answers 200 for global and one area, over a
short and an all-time window, and the app no longer issues KEYS.

  python test/smoke_counter_api.py      (stack up, ideally after some feeding)
"""
import json
import os
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as h  # noqa: E402

BASE = "http://127.0.0.1:15050"
CASES = [
    ("get_pokemon_counterseries", "totals", ("hourly", "daily", "weekly")),
    ("get_pokemon_counterseries", "tth", ("hourly", "daily", "weekly")),
    ("get_pokemon_counterseries", "weather", ("monthly",)),
    ("get_raids_counterseries", "totals", ("hourly", "daily", "weekly")),
    ("get_invasions_counterseries", "totals", ("hourly", "daily", "weekly")),
    ("get_quest_counterseries", "totals", ("hourly", "daily", "weekly")),
]


def get(path, **params):
    url = f"{BASE}/api/redis/{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]


def main() -> int:
    r = h.rds()
    keys_before = int(r.info("commandstats").get("cmdstat_keys", {}).get("calls", 0))
    bad = 0
    total = 0
    for path, ctype, intervals in CASES:
        for interval in intervals:
            for area in ("global", "TestArea01"):
                for start in ("24 hours", "2025-01-01T00:00:00"):
                    for mode in ("sum", "grouped"):
                        status, body = get(path, counter_type=ctype, interval=interval, area=area,
                                           start_time=start, end_time="now", mode=mode, response_format="json")
                        total += 1
                        if status != 200:
                            bad += 1
                            print(f"{status} {path} {ctype}/{interval} {area} {start} {mode}: {body}")
    keys_after = int(r.info("commandstats").get("cmdstat_keys", {}).get("calls", 0))
    print(f"{total - bad}/{total} requests OK; KEYS calls during the run: {keys_after - keys_before}")
    ok = not bad and keys_after == keys_before
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
