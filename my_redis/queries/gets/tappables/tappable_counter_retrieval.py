from datetime import datetime

from my_redis.connect_redis import RedisManager
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

FAMILIES = {
    "hourly": ("counter:tappable_hourly", "%Y%m%d%H"),
    "daily": ("counter:tappable_daily", "%Y%m%d"),
    "weekly": ("counter:tappable_total", "%Y%m%d"),
}


def _norm(x):
    if x is None or (isinstance(x, str) and x.lower() == "all"):
        return None
    if isinstance(x, str):
        return {x}
    return set(map(str, x))


def _breakdown(fields: dict) -> dict:
    """{type:reward_type:reward_id:total|amount: n} -> totals, per type and per reward."""
    out = {"total": 0, "amount": 0, "tappable_type": {}, "reward": {}}
    for field, n in fields.items():
        ttype, reward_type, reward_id, metric = field.split(":")
        out[metric] += n
        reward = out["reward"].setdefault(f"{reward_type}:{reward_id}", {"total": 0, "amount": 0})
        reward[metric] += n
        if metric == "total":
            out["tappable_type"][ttype] = out["tappable_type"].get(ttype, 0) + n
    out["reward"] = dict(sorted(out["reward"].items(), key=lambda kv: -kv[1]["total"]))
    return out


class TappableCounterRetrieval:
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 tappable_type="all", reward_type="all", reward_id="all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.types = _norm(tappable_type)
        self.reward_types = _norm(reward_type)
        self.reward_ids = _norm(reward_id)

    def _keep(self, field: str) -> bool:
        parts = field.split(":")
        if len(parts) != 4:
            return False
        ttype, reward_type, reward_id, _ = parts
        return ((self.types is None or ttype in self.types) and
                (self.reward_types is None or reward_type in self.reward_types) and
                (self.reward_ids is None or reward_id in self.reward_ids))

    async def retrieve(self, interval: str) -> dict:
        client = await redis_manager.check_redis_connection()
        if not client:
            return {"mode": self.mode, "data": {}}
        family, fmt = FAMILIES[interval]
        pattern = f"{family}:*" if self.area.lower() in ("global", "all") else f"{family}:{self.area}:*"
        keys = await filtering_keys.find_time_keys(client, pattern, fmt, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        grouped = await filtering_keys.aggregate_keys(keys, "grouped")
        buckets = {}
        for key, fields in grouped.items():
            stamp = key.rsplit(":", 1)[1]
            if self.mode == "sum":
                bucket = "all"
            elif self.mode == "surged":
                bucket = f"hour {int(stamp[8:10])}"
            elif len(stamp) == 10:
                bucket = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]} {stamp[8:10]}:00"
            else:
                bucket = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
            acc = buckets.setdefault(bucket, {})
            for f, n in fields.items():
                if self._keep(f):
                    acc[f] = acc.get(f, 0) + n

        if self.mode == "sum":
            return {"mode": "sum", "data": _breakdown(buckets.get("all", {}))}
        if self.mode == "surged":
            ordered = sorted(buckets.items(), key=lambda kv: int(kv[0].split()[1]))
        else:
            ordered = sorted(buckets.items())
        return {"mode": self.mode, "data": {b: _breakdown(f) for b, f in ordered}}
