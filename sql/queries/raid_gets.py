from __future__ import annotations
import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sql.connect_db import fetch_all
from sql.utils.sql_parsers import _build_in_clause
from sql.utils.time_parser import daterange_inclusive_days, clip_seen_window_for_day

@dataclass(frozen=True)
class RaidFilters:
    gyms: Optional[List[str]] = None
    raid_pokemon: Optional[List[int]] = None
    raid_form: Optional[List[int]] = None
    raid_level: Optional[List[int]] = None
    raid_team: Optional[List[int]] = None
    raid_costume: Optional[List[int]] = None
    raid_is_exclusive: Optional[List[int]] = None
    raid_ex_raid_eligible: Optional[List[int]] = None

# per-day SQL
async def fetch_raids_day(
    *,
    area_id: int,
    day: date,
    filters: RaidFilters,
    seen_from: Optional[datetime] = None,
    seen_to: Optional[datetime] = None,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    where: List[str] = ["r.area_id = %s", "r.day_date = %s"]
    params: List[Any] = [area_id, day]

    def _maybe(col: str, vals: Optional[List[Any]]):
        if vals:
            clause, v = _build_in_clause(col, vals)
            where.append(clause)
            params.extend(v)

    _maybe("r.gym", filters.gyms)
    _maybe("r.raid_pokemon", filters.raid_pokemon)
    _maybe("r.raid_form", filters.raid_form)
    _maybe("r.raid_level", filters.raid_level)
    _maybe("r.raid_team", filters.raid_team)
    _maybe("r.raid_costume", filters.raid_costume)
    _maybe("r.raid_is_exclusive", filters.raid_is_exclusive)
    _maybe("r.raid_ex_raid_eligible", filters.raid_ex_raid_eligible)

    clipped = clip_seen_window_for_day(day, seen_from, seen_to)
    if clipped:
        where.append("r.seen_at BETWEEN %s AND %s")
        params.extend([clipped[0], clipped[1]])

    where_sql = " AND ".join(where)
    limit_sql = f" LIMIT {int(limit)}" if limit and limit > 0 else ""

    sql = f"""
        SELECT
          g.gym_name,
          r.raid_pokemon,
          r.raid_form,
          r.raid_level,
          ANY_VALUE(g.latitude)  AS latitude,
          ANY_VALUE(g.longitude) AS longitude,
          COUNT(*) AS cnt
        FROM raids_daily_events AS r
        JOIN gyms AS g
          ON g.gym = r.gym
        WHERE {where_sql}
        GROUP BY r.gym, r.raid_pokemon, r.raid_form, r.raid_level
        ORDER BY cnt DESC, r.raid_pokemon ASC{limit_sql}
    """
    return await fetch_all(sql, params)

# range orchestrator
async def fetch_raids_range(
    *,
    area_id: int,
    area_name: str,
    seen_from: datetime,
    seen_to: datetime,
    filters: RaidFilters,
    limit_per_day: int = 0,
    concurrency: int = 4,
) -> Dict[str, Any]:
    days = daterange_inclusive_days(seen_from, seen_to)
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _task(d: date) -> List[Dict[str, Any]]:
        async with sem:
            return await fetch_raids_day(
                area_id=area_id,
                day=d,
                filters=filters,
                seen_from=seen_from,
                seen_to=seen_to,
                limit=limit_per_day,
            )

    per_day_lists = await asyncio.gather(*[asyncio.create_task(_task(d)) for d in days])

    # merge on gym, raid_pokemon, raid_form, raid_level
    acc: Dict[Tuple[str, int, int, int], Dict[str, Any]] = {}
    for rows in per_day_lists:
        for r in rows:
            key = (str(r["gym_name"]), int(r["raid_pokemon"]), int(r["raid_form"]), int(r["raid_level"]))
            if key not in acc:
                acc[key] = {
                    "gym_name": key[0],
                    "raid_pokemon": key[1],
                    "raid_form": key[2],
                    "raid_level": key[3],
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude"),
                    "count": int(r.get("cnt", 0)),
                }
            else:
                acc[key]["count"] += int(r.get("cnt", 0))

    data = list(acc.values())
    data.sort(key=lambda x: (-x["count"], x["raid_pokemon"]))

    return {
        "start_time": seen_from.isoformat(sep=" "),
        "end_time":   seen_to.isoformat(sep=" "),
        "start_date": days[0].isoformat(),
        "end_date":   days[-1].isoformat(),
        "area": area_name,
        "rows": len(data),
        "data": data,
    }
