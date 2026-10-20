"""
Shared helpers for the local test scenarios: stack control, readiness,
Redis / MariaDB access and container memory sampling.

All scenarios run on the host and talk to the stack in docker-compose.yml
through its mapped ports.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import subprocess
import threading
import time

import pymysql
import redis as _redis

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE_FILE = os.path.join(HERE, "docker-compose.yml")
RESULTS_DIR = os.path.join(HERE, "results")
APP_CONTAINER = "psyduck-test-app-1"
WEBHOOK_URL = "http://127.0.0.1:15050/webhook"
READY_MARKER = "Worker ready and accepting requests"


# ----- docker compose ---------------------------------------------------------
def compose(*args, check=True, capture=False, env=None) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, *args]
    return subprocess.run(cmd, check=check, text=True, encoding="utf-8", errors="replace",
                          capture_output=capture, env={**os.environ, **(env or {})})


def up(build: bool = False, env=None):
    os.makedirs(os.path.join(RESULTS_DIR, "applogs"), exist_ok=True)
    since = _now_iso()
    compose("up", "-d", *(["--build"] if build else []), env=env)
    wait_app_ready(since)


def down():
    compose("down", "-v", "--remove-orphans", check=False)


def restart_app(timeout: float = 300):
    since = _now_iso()
    compose("restart", "app")
    return wait_app_ready(since, timeout)


def stop(service: str):
    compose("stop", service)


def start(service: str):
    compose("start", service)


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def app_logs(since: str | None = None) -> str:
    args = ["logs", "--no-color", "app"]
    if since:
        args[1:1] = ["--since", since]
    return compose(*args, capture=True, check=False).stdout


def wait_app_ready(since: str, timeout: float = 300) -> float:
    """Block until the app logs its ready line after `since`; returns seconds waited."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if READY_MARKER in app_logs(since):
            return time.time() - t0
        time.sleep(2)
    raise TimeoutError(f"app not ready after {timeout}s:\n{app_logs(since)[-3000:]}")


# ----- memory -------------------------------------------------------------------
_UNITS = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1 / 1024, "MB": 1, "GB": 1024}


def app_mem_mb() -> float | None:
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", APP_CONTAINER],
                         capture_output=True, text=True).stdout.strip()
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", out)
    return float(m.group(1)) * _UNITS.get(m.group(2), 1) if m else None


def app_process_rss_mb() -> float | None:
    """RSS of the python process inside the container (excludes page cache)."""
    out = subprocess.run(["docker", "exec", APP_CONTAINER, "sh", "-c",
                          "grep VmRSS /proc/1/status 2>/dev/null || "
                          "grep VmRSS /proc/$(pgrep -f psyduckv2.py | head -1)/status"],
                         capture_output=True, text=True).stdout
    m = re.search(r"VmRSS:\s+(\d+)\s+kB", out)
    return int(m.group(1)) / 1024 if m else None


def app_process_peak_mb() -> float | None:
    out = subprocess.run(["docker", "exec", APP_CONTAINER, "sh", "-c",
                          "grep VmHWM /proc/1/status 2>/dev/null"],
                         capture_output=True, text=True).stdout
    m = re.search(r"VmHWM:\s+(\d+)\s+kB", out)
    return int(m.group(1)) / 1024 if m else None


class MemorySampler(threading.Thread):
    """Samples the app process RSS every `every` seconds until stop()."""

    def __init__(self, every: float = 2.0):
        super().__init__(daemon=True)
        self.every = every
        self.samples: list[tuple[float, float]] = []
        self._halt = threading.Event()
        self.t0 = time.time()

    def run(self):
        while not self._halt.is_set():
            v = app_process_rss_mb()
            if v is not None:
                self.samples.append((round(time.time() - self.t0, 1), round(v, 1)))
            self._halt.wait(self.every)

    def stop(self):
        self._halt.set()
        self.join(timeout=10)

    @property
    def peak(self) -> float | None:
        return max((v for _, v in self.samples), default=None)


# ----- data stores ---------------------------------------------------------------
def rds(db: int = 1) -> _redis.Redis:
    return _redis.Redis(host="127.0.0.1", port=16379, db=db, password="testpass", decode_responses=True)


def sql(query: str, params=None, database: str = "PsyduckV2"):
    conn = pymysql.connect(host="127.0.0.1", port=13306, user="psyduck", password="psyduck",
                           database=database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


def sql_val(query: str, params=None, database: str = "PsyduckV2"):
    rows = sql(query, params, database)
    return rows[0][0] if rows else None


def db_counts() -> dict:
    """Row counts the exact-mode feeder can be compared against."""
    return {
        "iv_rows": int(sql_val("SELECT COUNT(*) FROM pokemon_iv_daily_events") or 0),
        "shiny_total": int(sql_val("SELECT COALESCE(SUM(total_count),0) FROM shiny_username_rates") or 0),
        "raids": int(sql_val("SELECT COUNT(*) FROM raids_daily_events") or 0),
        "quests": int(sql_val("SELECT COUNT(*) FROM quests_item_daily_events") or 0),
        "invasions": int(sql_val("SELECT COUNT(*) FROM invasions_daily_events") or 0),
    }


def buffer_lengths() -> dict:
    r = rds()
    out = {}
    for k in r.scan_iter(match="buffer:*", count=1000):
        t = r.type(k)
        out[k] = r.llen(k) if t == "list" else r.hlen(k) if t == "hash" else t
    return out


# ----- Redis contention ------------------------------------------------------------
_BUSY_LUA = """
local ms = tonumber(ARGV[1])
local t = redis.call('TIME')
local start = t[1] * 1000000 + t[2]
repeat
  t = redis.call('TIME')
until (t[1] * 1000000 + t[2]) - start >= ms * 1000
return 1
"""


class RedisHog:
    """
    Keeps Redis's single thread busy with back-to-back Lua scripts, the way the
    production cleanup's EVALSHA chunks did on Oct 14. Redis stays responsive
    (no socket timeouts) but ingest capacity drops below the arrival rate.
    """

    def __init__(self, threads: int = 6, script_ms: int = 50):
        self.threads = threads
        self.script_ms = script_ms
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self.calls = 0

    def _loop(self):
        r = rds()
        sha = r.script_load(_BUSY_LUA)
        while not self._stop.is_set():
            r.evalsha(sha, 0, self.script_ms)
            self.calls += 1

    def __enter__(self):
        for _ in range(self.threads):
            t = threading.Thread(target=self._loop, daemon=True)
            t.start()
            self._workers.append(t)
        return self

    def __exit__(self, *exc):
        self._stop.set()
        for t in self._workers:
            t.join(timeout=5)


# ----- results -------------------------------------------------------------------
def write_result(name: str, data: dict) -> str:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    return path


def banner(text: str):
    print(f"\n{'=' * 78}\n{text}\n{'=' * 78}", flush=True)
