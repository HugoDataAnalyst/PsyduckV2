"""
/health against the running test stack (no API auth there, so only the status
and HTTP code are visible from outside):

  healthy          fresh webhook marks                      -> ok, 200
  stale webhooks   last webhook marks 1000 s old            -> degraded, 200 / 503 with strict
  shedding         received now, last accepted 1000 s ago   -> degraded
  buffer stuck     raid buffer last flushed 1000 s ago      -> degraded
  redis down       redis container stopped                  -> down, 503, within the check timeout
  docker           container health of the app              -> healthy

  python test/check_health.py      (stack up)
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import harness as h  # noqa: E402

URL = "http://127.0.0.1:15050/health"
WEBHOOK_KEY = "psyduckv2:health:last_webhook"
FLUSH_KEY = "psyduckv2:health:flush"


def get(strict: bool = False) -> tuple[int, str, float]:
    t0 = time.time()
    try:
        with urllib.request.urlopen(URL + ("?strict=true" if strict else ""), timeout=15) as r:
            code, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read()
    return code, json.loads(body)["status"], time.time() - t0


def wait_for(status: str, timeout: float = 180) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if get()[1] == status:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def main() -> int:
    r = h.rds()
    errors = []

    def expect(label, got, want):
        print(f"  {label}: {got}")
        if got != want:
            errors.append(f"{label}: {got} != {want}")

    now = int(time.time())
    r.hset(WEBHOOK_KEY, mapping={"received": now, "accepted": now})
    expect("healthy", get()[:2], (200, "ok"))

    r.hset(WEBHOOK_KEY, mapping={"received": now - 1000, "accepted": now - 1000})
    expect("stale webhooks", get()[:2], (200, "degraded"))
    expect("stale webhooks strict", get(strict=True)[:2], (503, "degraded"))

    r.hset(WEBHOOK_KEY, mapping={"received": now, "accepted": now - 1000})
    expect("shedding", get()[:2], (200, "degraded"))

    r.hset(WEBHOOK_KEY, mapping={"received": now, "accepted": now})
    r.hset(FLUSH_KEY, "buffer:raid_events", now - 1000)
    expect("buffer stuck", get()[:2], (200, "degraded"))
    r.hset(FLUSH_KEY, "buffer:raid_events", now)

    h.stop("redis")
    try:
        code, status, took = get()
        expect("redis down", (code, status), (503, "down"))
        if took > 5:
            errors.append(f"redis down answer took {took:.1f}s")
    finally:
        h.start("redis")
    if not wait_for("ok") and not wait_for("degraded", 5):
        errors.append("app did not recover after Redis came back")

    health = h.compose("ps", "--format", "{{.Health}}", "app", capture=True, check=False).stdout.strip()
    expect("docker health", health, "healthy")

    for e in errors:
        print("  FAIL:", e)
    print("RESULT:", "PASS" if not errors else "FAIL")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
