from __future__ import annotations
import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from sql.connect_db import fetch_all
from sql.utils.sql_parsers import _build_in_clause
from sql.utils.time_parser import daterange_inclusive_days, clip_seen_window_for_day

@dataclass(frozen=True)
class InvasionFilters:
    pokestops: Optional[List[str]] = None
    display_types: Optional[List[int]] = None
    characters: Optional[List[int]] = None
    grunts: Optional[List[int]] = None
    confirmed: Optional[List[int]] = None          # 0/1

# per-day SQL
async def fetch_invasions_day(
    *,
    area_id: int,
    day: date,
    filters: InvasionFilters,
    seen_from: Optional[datetime] = None,
    seen_to: Optional[datetime] = None,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    where: List[str] = ["i.area_id = %s", "i.day_date = %s"]
    params: List[Any] = [area_id, day]

    def _maybe(col: str, vals: Optional[List[Any]]):
        if vals:
            clause, v = _build_in_clause(col, vals)
            where.append(clause)
            params.extend(v)

    _maybe("i.pokestop", filters.pokestops)
    _maybe("i.display_type", filters.display_types)
    _maybe("i.`character`", filters.characters)
    _maybe("i.grunt", filters.grunts)
    _maybe("i.confirmed", filters.confirmed)

    clipped = clip_seen_window_for_day(day, seen_from, seen_to)
    if clipped:
        where.append("i.seen_at BETWEEN %s AND %s")
        params.extend([clipped[0], clipped[1]])

    where_sql = " AND ".join(where)
    limit_sql = f" LIMIT {int(limit)}" if limit and limit > 0 else ""

    sql = f"""
        SELECT
          p.pokestop_name,
          i.display_type,
          i.`character`,
          ANY_VALUE(p.latitude)  AS latitude,
          ANY_VALUE(p.longitude) AS longitude,
          COUNT(*) AS cnt
        FROM invasions_daily_events AS i
        JOIN pokestops AS p
          ON p.pokestop = i.pokestop
        WHERE {where_sql}
        GROUP BY i.pokestop, i.display_type, i.`character`
        ORDER BY cnt DESC, i.display_type ASC{limit_sql}
    """
    return await fetch_all(sql, params)

# range orchestrator
async def fetch_invasions_range(
    *,
    area_id: int,
    area_name: str,
    seen_from: datetime,
    seen_to: datetime,
    filters: InvasionFilters,
    limit_per_day: int = 0,
    concurrency: int = 4,
) -> Dict[str, Any]:
    days = daterange_inclusive_days(seen_from, seen_to)
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _task(d: date) -> List[Dict[str, Any]]:
        async with sem:
            return await fetch_invasions_day(
                area_id=area_id,
                day=d,
                filters=filters,
                seen_from=seen_from,
                seen_to=seen_to,
                limit=limit_per_day,
            )

    per_day_lists = await asyncio.gather(*[asyncio.create_task(_task(d)) for d in days])

    # merge on pokestop, display_type, character
    acc: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for rows in per_day_lists:
        for r in rows:
            key = (str(r["pokestop"]), int(r["display_type"]), int(r["character"]))
            if key not in acc:
                acc[key] = {
                    "pokestop": key[0],
                    "display_type": key[1],
                    "character": key[2],
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude"),
                    "count": int(r.get("cnt", 0)),
                }
            else:
                acc[key]["count"] += int(r.get("cnt", 0))

    data = list(acc.values())
    data.sort(key=lambda x: (-x["count"], x["display_type"]))

    return {
        "start_time": seen_from.isoformat(sep=" "),
        "end_time":   seen_to.isoformat(sep=" "),
        "start_date": days[0].isoformat(),
        "end_date":   days[-1].isoformat(),
        "area": area_name,
        "rows": len(data),
        "data": data,
    }
