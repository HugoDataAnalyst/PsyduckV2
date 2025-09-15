import re
import asyncio
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sql.connect_db import fetch_all
from sql.utils.time_parser import daterange_inclusive_days, clip_seen_window_for_day

# filters
@dataclass(frozen=True)
class HeatmapFilters:
    pokemon_ids: Optional[List[int]] = None
    forms: Optional[List[str]] = None
    iv_expr: Optional[str] = None
    level_expr: Optional[str] = None

_EXPR_RE = re.compile(r"^(>=|<=|==|=|>|<)\s*(\d+)$")

def _append_expr_csv(where_parts: List[str], params: List[Any], col: str, expr_csv: Optional[str]) -> None:
    if not expr_csv or expr_csv.lower() == "all":
        return
    inner, vals = [], []
    for raw in (x.strip() for x in expr_csv.split(",") if x.strip()):
        m = _EXPR_RE.match(raw)
        if not m:
            raise ValueError(f"Invalid expression '{raw}' for column {col}")
        op, val = m.group(1), int(m.group(2))
        if op == "==":
            op = "="
        inner.append(f"{col} {op} %s")
        vals.append(val)
    if inner:
        where_parts.append("(" + " OR ".join(inner) + ")")
        params.extend(vals)

# per-day SQL
async def fetch_pokemon_heatmap_day(
    *,
    area_id: int,
    day: date,
    filters: HeatmapFilters,
    seen_from: Optional[datetime] = None,
    seen_to: Optional[datetime] = None,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    where, params = ["e.area_id = %s", "e.day_date = %s"], [area_id, day]

    # IN filters - parsed in the API
    if filters.pokemon_ids:
        where.append(f"e.pokemon_id IN ({', '.join(['%s'] * len(filters.pokemon_ids))})")
        params.extend(filters.pokemon_ids)
    if filters.forms:
        where.append(f"e.form IN ({', '.join(['%s'] * len(filters.forms))})")
        params.extend(filters.forms)

    # threshold filters
    _append_expr_csv(where, params, "e.iv", filters.iv_expr)
    _append_expr_csv(where, params, "e.level", filters.level_expr)

    # seen_at clip for this specific day
    clipped = clip_seen_window_for_day(day, seen_from, seen_to)
    if clipped:
        where.append("e.seen_at BETWEEN %s AND %s")
        params.extend([clipped[0], clipped[1]])

    sql = f"""
        SELECT
          e.spawnpoint,
          e.pokemon_id,
          e.form,
          ANY_VALUE(s.latitude)  AS latitude,
          ANY_VALUE(s.longitude) AS longitude,
          COUNT(*) AS cnt
        FROM pokemon_iv_daily_events AS e
        JOIN spawnpoints AS s
          ON s.spawnpoint = e.spawnpoint
        WHERE {' AND '.join(where)}
        GROUP BY e.spawnpoint, e.pokemon_id, e.form
        ORDER BY cnt DESC, e.pokemon_id ASC
        {"LIMIT " + str(int(limit)) if limit and limit > 0 else ""}
    """
    return await fetch_all(sql, params)

# range orchestrator
async def fetch_pokemon_heatmap_range(
    *,
    area_id: int,
    seen_from: datetime,
    seen_to: datetime,
    filters: HeatmapFilters,
    limit_per_day: int = 0,
    concurrency: int = 4,
) -> Dict[str, Any]:
    days = daterange_inclusive_days(seen_from, seen_to)
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _task(d: date):
        async with sem:
            return await fetch_pokemon_heatmap_day(
                area_id=area_id,
                day=d,
                filters=filters,
                seen_from=seen_from,
                seen_to=seen_to,
                limit=limit_per_day,
            )

    per_day_lists = await asyncio.gather(*[asyncio.create_task(_task(d)) for d in days])

    # merge on spawnpoint, pokemon_id, form
    acc: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
    for rows in per_day_lists:
        for r in rows:
            key = (int(r["spawnpoint"]), int(r["pokemon_id"]), str(r["form"]))
            if key not in acc:
                acc[key] = {
                    "spawnpoint": key[0],
                    "pokemon_id": key[1],
                    "form": key[2],
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude"),
                    "count": int(r.get("cnt", 0)),
                }
            else:
                acc[key]["count"] += int(r.get("cnt", 0))

    data = list(acc.values())
    data.sort(key=lambda x: (-x["count"], x["pokemon_id"]))

    return {
        "start_time": seen_from.isoformat(sep=" "),
        "end_time": seen_to.isoformat(sep=" "),
        "start_date": days[0].isoformat(),
        "end_date": days[-1].isoformat(),
        "rows": len(data),
        "data": data,
    }
