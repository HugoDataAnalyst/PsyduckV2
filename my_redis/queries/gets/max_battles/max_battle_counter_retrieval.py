from datetime import datetime

from my_redis.connect_redis import RedisManager
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

FAMILIES = {"daily": "counter:max_battle_daily", "weekly": "counter:max_battle_total"}


def _norm(x):
    if x is None or (isinstance(x, str) and x.lower() == "all"):
        return None
    if isinstance(x, str):
        return {x}
    return set(map(str, x))


def _breakdown(fields: dict) -> dict:
    """{pid:level:form:costume:bread_mode:total: n} -> totals per level, bread mode, costume and boss."""
    out = {"total": 0, "battle_level": {}, "bread_mode": {}, "battle_costume": {}, "battle_pokemon+battle_form": {}}
    for field, n in fields.items():
        pid, level, form, costume, bread, _ = field.split(":")
        out["total"] += n
        for group, key in (("battle_level", level), ("bread_mode", bread), ("battle_costume", costume),
                           ("battle_pokemon+battle_form", f"{pid}:{form}")):
            out[group][key] = out[group].get(key, 0) + n
    for group in ("battle_level", "bread_mode", "battle_costume", "battle_pokemon+battle_form"):
        out[group] = dict(sorted(out[group].items(), key=lambda kv: -kv[1]))
    return out


class MaxBattleCounterRetrieval:
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 battle_pokemon="all", battle_form="all", battle_level="all", battle_costume="all", bread_mode="all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.pokemons = _norm(battle_pokemon)
        self.forms = _norm(battle_form)
        self.levels = _norm(battle_level)
        self.costumes = _norm(battle_costume)
        self.bread_modes = _norm(bread_mode)

    def _keep(self, field: str) -> bool:
        parts = field.split(":")
        if len(parts) != 6:
            return False
        pid, level, form, costume, bread, _ = parts
        return ((self.pokemons is None or pid in self.pokemons) and
                (self.levels is None or level in self.levels) and
                (self.forms is None or form in self.forms) and
                (self.costumes is None or costume in self.costumes) and
                (self.bread_modes is None or bread in self.bread_modes))

    async def retrieve(self, interval: str) -> dict:
        client = await redis_manager.check_redis_connection()
        if not client:
            return {"mode": self.mode, "data": {}}
        family = FAMILIES[interval]
        pattern = f"{family}:*" if self.area.lower() in ("global", "all") else f"{family}:{self.area}:*"
        keys = await filtering_keys.find_time_keys(client, pattern, "%Y%m%d", self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        grouped = await filtering_keys.aggregate_keys(keys, "grouped")
        if self.mode == "sum":
            merged = {}
            for fields in grouped.values():
                for f, n in fields.items():
                    if self._keep(f):
                        merged[f] = merged.get(f, 0) + n
            return {"mode": "sum", "data": _breakdown(merged)}

        per_bucket = {}
        for key, fields in grouped.items():
            stamp = key.rsplit(":", 1)[1]
            bucket = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
            acc = per_bucket.setdefault(bucket, {})
            for f, n in fields.items():
                if self._keep(f):
                    acc[f] = acc.get(f, 0) + n
        return {"mode": "grouped", "data": {b: _breakdown(f) for b, f in sorted(per_bucket.items())}}
