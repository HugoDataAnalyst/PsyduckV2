"""
Webhook ingest accounting, load-shedding decisions and a periodic health line.

Per worker process. The webhook router records every batch it accepts (and
when it finishes), sheds new batches while the worker is overloaded, and
HealthReporter logs one summary line per interval - in-flight work, shed and
aborted events, event-loop lag and process memory - so an ingest backlog shows
up in the logs within a minute instead of hours later as an OOM kill.
"""
from __future__ import annotations

import asyncio
import os
import time

import config as AppConfig
from utils.logger import logger


class IngestStats:
    def __init__(self):
        self.inflight_events = 0
        self._reset_interval()

    def _reset_interval(self):
        self.peak_inflight = self.inflight_events
        self.accepted_events = 0
        self.accepted_posts = 0
        self.shed_events = 0
        self.shed_posts = 0
        self.aborted_events = 0

    def begin(self, n: int):
        self.inflight_events += n
        self.accepted_events += n
        self.accepted_posts += 1
        if self.inflight_events > self.peak_inflight:
            self.peak_inflight = self.inflight_events

    def end(self, n: int):
        self.inflight_events = max(0, self.inflight_events - n)

    def shed(self, n: int):
        self.shed_events += n
        self.shed_posts += 1

    def aborted(self, n: int):
        self.aborted_events += n

    def take_interval(self) -> dict:
        snap = {
            "inflight": self.inflight_events,
            "peak": self.peak_inflight,
            "accepted": self.accepted_events,
            "accepted_posts": self.accepted_posts,
            "shed": self.shed_events,
            "shed_posts": self.shed_posts,
            "aborted": self.aborted_events,
        }
        self._reset_interval()
        return snap


ingest = IngestStats()

try:
    _PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
except (AttributeError, ValueError, OSError):
    _PAGE_SIZE = 4096
_rss_cache: tuple[float, float | None] = (0.0, None)


def rss_mb() -> float | None:
    """Resident memory of this process (Linux /proc; None elsewhere). Cached for 1s."""
    global _rss_cache
    now = time.monotonic()
    if now - _rss_cache[0] < 1.0:
        return _rss_cache[1]
    try:
        with open("/proc/self/statm") as f:
            value = int(f.read().split()[1]) * _PAGE_SIZE / 1048576
    except Exception:
        value = None
    _rss_cache = (now, value)
    return value


def overload_reason() -> str | None:
    """Why this worker should refuse new work right now, or None if it is fine."""
    cap = AppConfig.webhook_max_inflight_events
    if cap > 0 and ingest.inflight_events >= cap:
        return f"{ingest.inflight_events} events in flight (cap {cap})"
    limit = AppConfig.webhook_max_rss_mb
    if limit > 0:
        rss = rss_mb()
        if rss is not None and rss >= limit:
            return f"process memory {rss:.0f} MB (cap {limit} MB)"
    return None


class RateLimitedWarning:
    """Logs at most one warning per `every` seconds and counts what it skipped."""

    def __init__(self, every: float = 30.0):
        self.every = every
        self._last = 0.0
        self._skipped = 0

    def __call__(self, message: str):
        now = time.monotonic()
        if now - self._last >= self.every:
            extra = f" (+{self._skipped} similar in the last {self.every:.0f}s)" if self._skipped else ""
            logger.warning(message + extra)
            self._last = now
            self._skipped = 0
        else:
            self._skipped += 1


class HealthReporter:
    """Logs one ingest health line per interval (WARNING when anything is wrong)."""

    LAG_PROBE_S = 0.5
    LAG_WARN_S = 1.0

    def __init__(self, interval: int = 60):
        self.interval = max(5, int(interval))
        self._max_lag = 0.0
        self._tasks: list[asyncio.Task] = []

    async def _lag_probe(self):
        loop = asyncio.get_running_loop()
        while True:
            t0 = loop.time()
            await asyncio.sleep(self.LAG_PROBE_S)
            lag = loop.time() - t0 - self.LAG_PROBE_S
            if lag > self._max_lag:
                self._max_lag = lag

    async def _report(self):
        while True:
            await asyncio.sleep(self.interval)
            s = ingest.take_interval()
            lag, self._max_lag = self._max_lag, 0.0
            rss = rss_mb()
            line = (
                f"🩺 ingest | in-flight {s['inflight']:,} ev (peak {s['peak']:,}) | "
                f"last {self.interval}s: accepted {s['accepted']:,} ev in {s['accepted_posts']:,} POSTs, "
                f"shed {s['shed']:,} ev ({s['shed_posts']:,} POSTs), aborted {s['aborted']:,} | "
                f"loop lag max {lag * 1000:,.0f} ms"
                + (f" | RSS {rss:,.0f} MB" if rss is not None else "")
            )
            troubled = s["shed"] or s["aborted"] or lag >= self.LAG_WARN_S
            (logger.warning if troubled else logger.info)(line)

    async def start(self):
        self._tasks = [asyncio.create_task(self._lag_probe()), asyncio.create_task(self._report())]

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks = []
