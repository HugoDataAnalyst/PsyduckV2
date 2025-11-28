from __future__ import annotations
import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from sql.connect_db import fetch_all
from sql.utils.time_parser import daterange_inclusive_days, clip_seen_window_for_day
from sql.utils.sql_parsers import _build_in_clause

# filters

@dataclass(frozen=True)
class QuestItemFilters:
    pokestops: Optional[List[str]] = None
    allowed_modes: Optional[List[int]] = None       # None | [1] | [0]
    # mode-coupled filters:
    ar_task_types: Optional[List[int]] = None
    normal_task_types: Optional[List[int]] = None
    ar_item_ids: Optional[List[int]] = None
    normal_item_ids: Optional[List[int]] = None

@dataclass(frozen=True)
class QuestMonFilters:
    pokestops: Optional[List[str]] = None
    allowed_modes: Optional[List[int]] = None       # None | [1] | [0]
    # mode-coupled filters (often None):
    ar_task_types: Optional[List[int]] = None
    normal_task_types: Optional[List[int]] = None
    ar_poke_ids: Optional[List[int]] = None
    normal_poke_ids: Optional[List[int]] = None

# helpers

def _append_or_mode_pairs_items(
    where: List[str], params: List[Any], alias: str,
    ar_task_types: Optional[List[int]],
    normal_task_types: Optional[List[int]],
    ar_item_ids: Optional[List[int]],
    normal_item_ids: Optional[List[int]],
) -> None:
    disj = []
    if ar_task_types is not None or ar_item_ids is not None:
        parts, p = [f"{alias}.mode = 1"], []
        if ar_task_types:
            sql, vals = _build_in_clause(f"{alias}.task_type", ar_task_types); parts.append(sql); p += vals

        if ar_item_ids:
            sql, vals = _build_in_clause(f"{alias}.item_id", ar_item_ids); parts.append(sql); p += vals

        disj.append((" AND ".join(parts), p))

    if normal_task_types is not None or normal_item_ids is not None:
        parts, p = [f"{alias}.mode = 0"], []
        if normal_task_types:
            sql, vals = _build_in_clause(f"{alias}.task_type", normal_task_types); parts.append(sql); p += vals

        if normal_item_ids:
            sql, vals = _build_in_clause(f"{alias}.item_id", normal_item_ids); parts.append(sql); p += vals

        disj.append((" AND ".join(parts), p))

    if disj:
        where.append("(" + " OR ".join([f"({d[0]})" for d in disj]) + ")")
        for _, ps in disj: params.extend(ps)

def _append_or_mode_pairs_pokemon(
    where: List[str], params: List[Any], alias: str,
    ar_task_types: Optional[List[int]],
    normal_task_types: Optional[List[int]],
    ar_poke_ids: Optional[List[int]],
    normal_poke_ids: Optional[List[int]],
) -> None:
    disj = []
    if ar_task_types is not None or ar_poke_ids is not None:
        parts, p = [f"{alias}.mode = 1"], []
        if ar_task_types:
            sql, vals = _build_in_clause(f"{alias}.task_type", ar_task_types); parts.append(sql); p += vals

        if ar_poke_ids:
            sql, vals = _build_in_clause(f"{alias}.poke_id", ar_poke_ids); parts.append(sql); p += vals

        disj.append((" AND ".join(parts), p))

    if normal_task_types is not None or normal_poke_ids is not None:
        parts, p = [f"{alias}.mode = 0"], []
        if normal_task_types:
            sql, vals = _build_in_clause(f"{alias}.task_type", normal_task_types); parts.append(sql); p += vals

        if normal_poke_ids:
            sql, vals = _build_in_clause(f"{alias}.poke_id", normal_poke_ids); parts.append(sql); p += vals

        disj.append((" AND ".join(parts), p))

    if disj:
        where.append("(" + " OR ".join([f"({d[0]})" for d in disj]) + ")")
        for _, ps in disj: params.extend(ps)

#  per-day SQL

async def fetch_quests_items_day(
    *, area_id: int, day: date, filters: QuestItemFilters,
    seen_from: Optional[datetime] = None, seen_to: Optional[datetime] = None,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    where: List[str] = ["i.area_id = %s", "i.day_date = %s"]
    params: List[Any] = [area_id, day]

    if filters.pokestops:
        sql, vals = _build_in_clause("i.pokestop", filters.pokestops); where.append(sql); params += vals
    if filters.allowed_modes:
        sql, vals = _build_in_clause("i.mode", filters.allowed_modes); where.append(sql); params += vals

    _append_or_mode_pairs_items(where, params, "i",
        filters.ar_task_types, filters.normal_task_types,
        filters.ar_item_ids,  filters.normal_item_ids)

    clipped = clip_seen_window_for_day(day, seen_from, seen_to)
    if clipped:
        where.append("i.seen_at BETWEEN %s AND %s"); params += [clipped[0], clipped[1]]

    sql = f"""
        SELECT
          p.pokestop_name,
          i.mode,
          i.task_type,
          ANY_VALUE(p.latitude)  AS latitude,
          ANY_VALUE(p.longitude) AS longitude,
          COUNT(*) AS cnt
        FROM quests_item_daily_events AS i
        LEFT JOIN pokestops AS p ON p.pokestop = i.pokestop
        WHERE {' AND '.join(where)}
        GROUP BY i.pokestop, i.mode, i.task_type
        {"LIMIT " + str(int(limit)) if limit and limit > 0 else ""}
    """
    return await fetch_all(sql, params)

async def fetch_quests_pokemon_day(
    *, area_id: int, day: date, filters: QuestMonFilters,
    seen_from: Optional[datetime] = None, seen_to: Optional[datetime] = None,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    where: List[str] = ["qp.area_id = %s", "qp.day_date = %s"]
    params: List[Any] = [area_id, day]

    if filters.pokestops:
        sql, vals = _build_in_clause("qp.pokestop", filters.pokestops); where.append(sql); params += vals

    if filters.allowed_modes:
        sql, vals = _build_in_clause("qp.mode", filters.allowed_modes); where.append(sql); params += vals

    _append_or_mode_pairs_pokemon(where, params, "qp",
        filters.ar_task_types, filters.normal_task_types,
        filters.ar_poke_ids,  filters.normal_poke_ids)

    clipped = clip_seen_window_for_day(day, seen_from, seen_to)
    if clipped:
        where.append("qp.seen_at BETWEEN %s AND %s"); params += [clipped[0], clipped[1]]

    sql = f"""
        SELECT
          qp.pokestop,
          qp.mode,
          qp.task_type,
          ANY_VALUE(p.latitude)  AS latitude,
          ANY_VALUE(p.longitude) AS longitude,
          COUNT(*) AS cnt
        FROM quests_pokemon_daily_events AS qp
        LEFT JOIN pokestops AS p ON p.pokestop = qp.pokestop
        WHERE {' AND '.join(where)}
        GROUP BY qp.pokestop, qp.mode, qp.task_type
        {"LIMIT " + str(int(limit)) if limit and limit > 0 else ""}
    """
    return await fetch_all(sql, params)

# range orchestrator

async def fetch_quests_range(
    *, area_id: int, area_name: str, seen_from: datetime, seen_to: datetime,
    items_filters: QuestItemFilters, mon_filters: QuestMonFilters,
    limit_per_day: int = 0, concurrency: int = 4,
) -> Dict[str, Any]:
    days = daterange_inclusive_days(seen_from, seen_to)
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _items_task(d: date):
        async with sem:
            return await fetch_quests_items_day(
                area_id=area_id, day=d, filters=items_filters,
                seen_from=seen_from, seen_to=seen_to, limit=limit_per_day,
            )

    async def _mon_task(d: date):
        async with sem:
            return await fetch_quests_pokemon_day(
                area_id=area_id, day=d, filters=mon_filters,
                seen_from=seen_from, seen_to=seen_to, limit=limit_per_day,
            )

    items_lists, mons_lists = await asyncio.gather(
        asyncio.gather(*[asyncio.create_task(_items_task(d)) for d in days]),
        asyncio.gather(*[asyncio.create_task(_mon_task(d)) for d in days]),
    )

    # merge items on pokestop, mode, task_type
    items_acc: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for rows in items_lists:
        for r in rows:
            key = (str(r["pokestop"]), int(r["mode"]), int(r["task_type"]))
            items_acc.setdefault(key, {
                "pokestop": key[0], "mode": key[1], "task_type": key[2],
                "latitude": r.get("latitude"), "longitude": r.get("longitude"),
                "count": 0,
            })["count"] += int(r.get("cnt", 0))
    items_data = list(items_acc.values())
    items_data.sort(key=lambda x: (x["mode"], -x["count"], x["task_type"]))

    # merge mons on pokestop, mode, task_type
    mons_acc: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for rows in mons_lists:
        for r in rows:
            key = (str(r["pokestop"]), int(r["mode"]), int(r["task_type"]))
            mons_acc.setdefault(key, {
                "pokestop": key[0], "mode": key[1], "task_type": key[2],
                "latitude": r.get("latitude"), "longitude": r.get("longitude"),
                "count": 0,
            })["count"] += int(r.get("cnt", 0))

    mons_data = list(mons_acc.values())
    mons_data.sort(key=lambda x: (x["mode"], -x["count"], x["task_type"]))

    return {
        "start_time": seen_from.isoformat(sep=" "),
        "end_time":   seen_to.isoformat(sep=" "),
        "start_date": days[0].isoformat(),
        "end_date":   days[-1].isoformat(),
        "area": area_name,
        "items":   {"rows": len(items_data), "data": items_data},
        "pokemon": {"rows": len(mons_data),  "data": mons_data},
    }
