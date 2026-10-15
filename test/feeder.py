"""
Synthetic Golbat webhook feeder.

Mimics Golbat's sender (webhooks/sender.go + webhook.go):
  * one POST per interval (default 1s) carrying every event collected since
    the last one, as a JSON list of {"type": ..., "message": {...}};
  * each POST runs in its own task, so a slow receiver gets overlapping
    requests exactly as it does in production;
  * no client timeout and no retry - the response status is ignored by Golbat,
    so a failed or rejected batch is simply gone.

Modes
  realistic  production-like mix: ~50% of Pokemon are encounters, the rest
             wild / nearby_cell / nearby_stop (rejected by PsyduckV2), ~10% of
             encounters are re-sends, plus a few raids, quests and invasions.
  exact      every event is valid and unique (one SQL row each), so callers can
             compare database row counts against `expected` to the row.

Usage
  python test/feeder.py --rate 400 --duration 60
  python test/feeder.py --rate 200 --duration 30 --mode exact
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field

import httpx

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
from areas import AREAS, random_point  # noqa: E402

DEFAULT_URL = "http://127.0.0.1:15050/webhook"

# seen_type split observed in production's sample feed (547 messages)
REALISTIC_SEEN_TYPES = [("encounter", 270), ("wild", 145), ("nearby_cell", 70), ("nearby_stop", 62)]
DEFAULT_MIX = {"pokemon": 0.95, "raid": 0.02, "quest": 0.02, "invasion": 0.01}


@dataclass
class Expected:
    """What the database should end up holding for the accepted (HTTP 200) POSTs."""
    iv_rows: int = 0          # pokemon_iv_daily_events rows (unique spawnpoint+seen_at)
    shiny_total: int = 0      # SUM(total_count) in shiny_username_rates
    raids: int = 0            # raids_daily_events rows
    quests: int = 0           # quests_item_daily_events rows
    invasions: int = 0        # invasions_daily_events rows


@dataclass
class Stats:
    posts: int = 0
    events_sent: int = 0
    events_accepted: int = 0
    events_rejected: int = 0
    status: Counter = field(default_factory=Counter)
    latencies: list = field(default_factory=list)
    expected: Expected = field(default_factory=Expected)


class Feeder:
    def __init__(self, url: str = DEFAULT_URL, rate: float = 400, interval: float = 1.0,
                 mode: str = "realistic", timeout: float | None = None, seed: int | None = None,
                 mix: dict | None = None, pokemon_ids: tuple[int, int] = (1, 1000),
                 forms: list[int] | None = None):
        # pokemon_ids / forms: a narrow range makes events hit the SAME counter
        # fields repeatedly, the way production's few hundred common species do.
        self.pokemon_ids = pokemon_ids
        self.forms = forms
        self.url = url
        self.rate = rate
        self.interval = interval
        self.mode = mode
        self.timeout = timeout
        self.rng = random.Random(seed)
        self.mix = mix or DEFAULT_MIX
        self.stats = Stats()
        self.pending: set[asyncio.Task] = set()
        self._run_id = f"{self.rng.getrandbits(24):06x}"
        self._seq = 0
        self._recent_encounters: list[dict] = []
        self._client: httpx.AsyncClient | None = None

    # ----- event builders -------------------------------------------------
    def _next(self) -> int:
        self._seq += 1
        return self._seq

    def _hex_id(self, width: int = 12) -> str:
        # run id + sequence keeps spawnpoints / forts unique across a run
        return f"{self._run_id}{self._next():0{width - 6}x}"

    def pokemon(self, seen_type: str = "encounter") -> dict:
        now = int(time.time())
        lat, lon, _ = random_point(self.rng)
        first_seen = now - self.rng.randint(0, 300)
        lifetime = self.rng.randint(600, 3400)
        msg = {
            "spawnpoint_id": self._hex_id(),
            "pokestop_id": "None",
            "pokestop_name": None,
            "encounter_id": str(self.rng.getrandbits(63) | (1 << 62)),
            "pokemon_id": self.rng.randint(*self.pokemon_ids),
            "latitude": lat,
            "longitude": lon,
            "disappear_time": first_seen + lifetime,
            "disappear_time_verified": self.rng.random() < 0.75,
            "first_seen": first_seen,
            "last_modified_time": now,
            "gender": self.rng.randint(1, 2),
            "form": self.rng.choice(self.forms) if self.forms else self.rng.randint(0, 3000),
            "costume": 0,
            "weather": self.rng.randint(0, 7),
            "username": f"acct{self.rng.randint(1, 500):04d}",
            "display_pokemon_id": None,
            "display_pokemon_form": None,
            "is_event": 0,
            "seen_type": seen_type,
            "capture_1": 0, "capture_2": 0, "capture_3": 0,
        }
        if seen_type in ("encounter", "lure_encounter", "tappable_encounter", "tappable_lure_encounter"):
            atk, dfn, sta = (self.rng.randint(0, 15) for _ in range(3))
            msg.update({
                "individual_attack": atk, "individual_defense": dfn, "individual_stamina": sta,
                "cp": self.rng.randint(10, 3500), "pokemon_level": self.rng.randint(1, 35),
                "move_1": 200, "move_2": 100, "weight": 10.0, "height": 1.0,
                "size": self.rng.randint(1, 5), "shiny": self.rng.random() < 0.02,
                "pvp": self._pvp(),
            })
        else:
            msg.update({
                "individual_attack": None, "individual_defense": None, "individual_stamina": None,
                "cp": None, "pokemon_level": None, "move_1": None, "move_2": None,
                "weight": None, "height": None, "size": None, "shiny": None, "pvp": None,
            })
        return msg

    def _pvp(self):
        if self.rng.random() > 0.3:
            return None
        def league():
            rank = 1 if self.rng.random() < 0.05 else self.rng.randint(2, 4000)
            return [{"pokemon": 1, "form": 0, "cap": 50, "value": 1, "level": 20,
                     "cp": 1400, "percentage": 0.9, "rank": rank, "capped": True}]
        return {"great": league(), "little": league(), "ultra": league()}

    def raid(self) -> dict:
        now = int(time.time())
        lat, lon, _ = random_point(self.rng)
        return {
            "gym_id": f"gym{self._hex_id()}", "gym_name": "Test Gym", "team_id": self.rng.randint(0, 3),
            "ex_raid_eligible": False, "is_exclusive": False, "level": self.rng.choice([1, 3, 5]),
            "pokemon_id": self.rng.randint(1, 1000), "form": 0, "costume": 0,
            "latitude": lat, "longitude": lon,
            "spawn": now - 600, "start": now - 60, "end": now + 2700,
            "rsvps": None, "cp": 20000, "gender": 1, "move_1": 1, "move_2": 2,
        }

    def quest(self) -> dict:
        now = int(time.time())
        lat, lon, _ = random_point(self.rng)
        return {
            "type": self.rng.randint(1, 60), "with_ar": self.rng.random() < 0.5,
            "latitude": lat, "longitude": lon,
            "pokestop_id": f"qs{self._hex_id()}", "pokestop_name": "Test Stop",
            "updated": now, "target": 1, "template": "t", "conditions": [],
            "rewards": [{"type": 2, "info": {"item_id": self.rng.randint(1, 1400), "amount": 3}}],
        }

    def invasion(self) -> dict:
        now = int(time.time())
        lat, lon, _ = random_point(self.rng)
        return {
            "pokestop_id": f"is{self._hex_id()}", "pokestop_name": "Test Stop",
            "latitude": lat, "longitude": lon, "start": now - 60,
            "incident_expire_timestamp": now + 1800,
            "display_type": 1, "character": self.rng.randint(4, 50),
            "grunt_type": self.rng.randint(4, 50), "confirmed": True,
        }

    def build_batch(self, n: int) -> tuple[list, Expected]:
        """Return (payload, expected-if-accepted) for one POST."""
        exp = Expected()
        batch = []
        types = list(self.mix)
        weights = [self.mix[t] for t in types]
        for _ in range(n):
            kind = self.rng.choices(types, weights)[0]
            if kind == "pokemon":
                if self.mode == "realistic" and self._recent_encounters and self.rng.random() < 0.10:
                    # re-send of an encounter already delivered (Golbat does this)
                    batch.append({"type": "pokemon", "message": dict(self.rng.choice(self._recent_encounters))})
                    continue
                st = "encounter" if self.mode == "exact" else \
                    self.rng.choices([s for s, _ in REALISTIC_SEEN_TYPES], [w for _, w in REALISTIC_SEEN_TYPES])[0]
                msg = self.pokemon(st)
                batch.append({"type": "pokemon", "message": msg})
                if st == "encounter":
                    exp.iv_rows += 1
                    exp.shiny_total += 1
                    self._recent_encounters.append(msg)
                    if len(self._recent_encounters) > 2000:
                        self._recent_encounters = self._recent_encounters[-1000:]
            elif kind == "raid":
                batch.append({"type": "raid", "message": self.raid()}); exp.raids += 1
            elif kind == "quest":
                batch.append({"type": "quest", "message": self.quest()}); exp.quests += 1
            else:
                batch.append({"type": "invasion", "message": self.invasion()}); exp.invasions += 1
        return batch, exp

    # ----- sending --------------------------------------------------------
    async def _post(self, payload: list, exp: Expected):
        t0 = time.perf_counter()
        try:
            resp = await self._client.post(self.url, content=json.dumps(payload),
                                           headers={"Content-Type": "application/json", "X-Golbat": "hey!"})
            code = resp.status_code
        except Exception as e:
            code = f"error:{type(e).__name__}"
        self.stats.latencies.append(time.perf_counter() - t0)
        self.stats.status[code] += 1
        if code == 200:
            self.stats.events_accepted += len(payload)
            e = self.stats.expected
            e.iv_rows += exp.iv_rows; e.shiny_total += exp.shiny_total
            e.raids += exp.raids; e.quests += exp.quests; e.invasions += exp.invasions
        else:
            self.stats.events_rejected += len(payload)

    async def run(self, duration: float, on_tick=None):
        """Send for `duration` seconds. POSTs still in flight are left running (see drain)."""
        if self._client is None:
            limits = httpx.Limits(max_connections=None, max_keepalive_connections=50)
            self._client = httpx.AsyncClient(timeout=self.timeout, limits=limits)
        per_post = max(1, int(round(self.rate * self.interval)))
        start = time.monotonic()
        tick = 0
        while time.monotonic() - start < duration:
            payload, exp = self.build_batch(per_post)
            self.stats.posts += 1
            self.stats.events_sent += len(payload)
            task = asyncio.create_task(self._post(payload, exp))
            self.pending.add(task)
            task.add_done_callback(self.pending.discard)
            tick += 1
            if on_tick:
                on_tick(tick, self)
            await asyncio.sleep(max(0.0, start + tick * self.interval - time.monotonic()))

    async def drain(self, timeout: float = 120):
        if self.pending:
            await asyncio.wait(set(self.pending), timeout=timeout)

    async def close(self):
        for t in list(self.pending):
            t.cancel()
        if self._client:
            await self._client.aclose()

    def summary(self) -> dict:
        lat = sorted(self.stats.latencies)
        q = lambda p: round(lat[min(len(lat) - 1, int(len(lat) * p))], 3) if lat else None
        return {
            "posts": self.stats.posts,
            "in_flight_posts": len(self.pending),
            "events_sent": self.stats.events_sent,
            "events_accepted": self.stats.events_accepted,
            "events_rejected": self.stats.events_rejected,
            "status": {str(k): v for k, v in self.stats.status.items()},
            "latency_s": {"p50": q(0.5), "p95": q(0.95), "max": round(lat[-1], 3) if lat else None,
                          "mean": round(statistics.mean(lat), 3) if lat else None},
            "expected": vars(self.stats.expected),
        }


async def _main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=400, help="events per second")
    ap.add_argument("--duration", type=float, default=30, help="seconds to send for")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between POSTs (Golbat: 1)")
    ap.add_argument("--mode", choices=["realistic", "exact"], default="realistic")
    ap.add_argument("--timeout", type=float, default=None, help="client timeout (Golbat: none)")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    f = Feeder(args.url, args.rate, args.interval, args.mode, args.timeout, args.seed)

    def tick(n, fd):
        if n % 5 == 0:
            s = fd.stats
            print(f"[{n:4d}s] posts={s.posts} in_flight={len(fd.pending)} status={dict(s.status)}", flush=True)

    try:
        await f.run(args.duration, on_tick=tick)
        await f.drain(timeout=60)
    finally:
        await f.close()
    print(json.dumps(f.summary(), indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
