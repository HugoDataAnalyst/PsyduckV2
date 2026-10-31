"""
Weather reads from weather_daily_events.

Golbat sends a cell's weather only when it changes, so a row means "from
seen_at on, this cell had this condition" - until the cell's next row. These
helpers turn the rows into intervals, clip them to the requested window and
split them per hour:

  cell_hours   hours a condition was in force, summed over cells (an area with
               6 cells has 6 cell-hours per hour)
  changes      condition changes inside the window (a first row or a repeat
               of the previous condition is not one)

The condition in force when the window opens comes from each cell's last row
before it (looked up to LOOKBACK_DAYS back). A cell's latest condition is taken
to last until the window's end or now, whichever is earlier. seen_at, like
every event table, is in the area's local time. A cell overlapping several
areas has rows in each; area=global counts it once.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from sql.connect_db import fetch_all

CONDITIONS = {0: "NONE", 1: "CLEAR", 2: "RAINY", 3: "PARTLY_CLOUDY", 4: "OVERCAST", 5: "WINDY", 6: "SNOW", 7: "FOG"}
LOOKBACK_DAYS = 7
HOUR = timedelta(hours=1)


def _name(condition: int, raw: bool):
    return condition if raw else CONDITIONS.get(condition, str(condition))


async def _cell_rows(area_ids: list[int] | None, start: datetime, end: datetime) -> dict:
    """{(area_id, s2_cell_id): [(seen_at, condition, lat, lon), ...]} sorted by time."""
    where = ["day_date BETWEEN %s AND %s", "seen_at < %s"]
    params: list = [(start - timedelta(days=LOOKBACK_DAYS)).date(), end.date(), end]
    if area_ids is not None:
        where.append(f"area_id IN ({', '.join(['%s'] * len(area_ids))})")
        params += area_ids
    rows = await fetch_all(
        "SELECT area_id, s2_cell_id, gameplay_condition, seen_at, latitude, longitude "
        f"FROM weather_daily_events WHERE {' AND '.join(where)} ORDER BY s2_cell_id, seen_at",
        params,
    )
    cells = defaultdict(list)
    for r in rows:
        cells[(int(r["area_id"]), int(r["s2_cell_id"]))].append(
            (r["seen_at"], int(r["gameplay_condition"]), float(r["latitude"]), float(r["longitude"])))
    return cells


def _intervals(events: list, start: datetime, end: datetime, now: datetime):
    """Yield (from, to, condition) of one cell, clipped to [start, end) and to now."""
    stop = min(end, now)
    keep = [e for e in events if e[0] < stop]
    # drop everything before the last change at or before start (that one carries in)
    first = max((i for i, e in enumerate(keep) if e[0] <= start), default=0)
    keep = keep[first:]
    for i, (t, cond, _, _) in enumerate(keep):
        t_next = keep[i + 1][0] if i + 1 < len(keep) else stop
        a, b = max(t, start), min(t_next, stop)
        if a < b:
            yield a, b, cond


def _change_times(events: list, start: datetime, end: datetime) -> list[datetime]:
    """
    Times in [start, end) where a cell's condition really changed. A cell's
    first row (a seed or first sighting) and a row repeating the previous
    condition (a warning change, or a sync row next to its webhook) are not
    changes.
    """
    out = []
    for (_, prev, *_), (t, cond, *_) in zip(events, events[1:]):
        if cond != prev and start <= t < end:
            out.append(t)
    return out


def _hour_pieces(a: datetime, b: datetime):
    """Split [a, b) at hour boundaries: yield (hour_start, hours)."""
    h = a.replace(minute=0, second=0, microsecond=0)
    while h < b:
        lo, hi = max(a, h), min(b, h + HOUR)
        yield h, (hi - lo).total_seconds() / 3600
        h += HOUR


def _bucket(hour: datetime, interval: str) -> str:
    if interval == "day":
        return hour.strftime("%Y-%m-%d")
    if interval == "week":
        return (hour - timedelta(days=hour.weekday())).strftime("%Y-%m-%d")
    return hour.strftime("%Y-%m")


async def fetch_weather_summary(*, area_ids: list[int] | None, offsets: dict[int, int], start: datetime,
                                end: datetime, interval: str, mode: str, raw: bool = False) -> dict:
    """mode=sum: cell-hours and % per condition per day/week/month; mode=grouped: per hour."""
    cells = await _cell_rows(area_ids, start, end)
    if area_ids is None:
        # a cell overlapping several areas is stored once per area: count it once
        first = {}
        for area_id, cell in sorted(cells):
            first.setdefault(cell, area_id)
        cells = {k: v for k, v in cells.items() if first[k[1]] == k[0]}
    utc_now = datetime.utcnow()
    hours = defaultdict(lambda: defaultdict(float))      # bucket -> condition -> cell_hours
    changes = defaultdict(int)
    seen_cells = set()
    for (area_id, cell), events in cells.items():
        now = utc_now + timedelta(hours=offsets.get(area_id, 0))
        for a, b, cond in _intervals(events, start, end, now):
            seen_cells.add(cell)
            for hour, h in _hour_pieces(a, b):
                key = hour.strftime("%Y-%m-%d %H:00") if mode == "grouped" else _bucket(hour, interval)
                hours[key][cond] += h
        for t in _change_times(events, start, end):
            key = t.strftime("%Y-%m-%d %H:00") if mode == "grouped" else _bucket(t, interval)
            changes[key] += 1

    data = {}
    for key in sorted(set(hours) | set(changes)):
        by_cond = hours.get(key, {})
        total = sum(by_cond.values())
        if mode == "grouped":
            entry = {_name(c, raw): round(v, 2) for c, v in sorted(by_cond.items(), key=lambda kv: -kv[1])}
            entry = {"dominant": _name(max(by_cond, key=by_cond.get), raw) if by_cond else None, **entry}
        else:
            entry = {_name(c, raw): {"cell_hours": round(v, 2), "pct": round(100 * v / total, 1) if total else 0.0}
                     for c, v in sorted(by_cond.items(), key=lambda kv: -kv[1])}
        entry["changes"] = changes.get(key, 0)
        data[key] = entry
    return {"cells": len(seen_cells), "data": data}


async def fetch_weather_cells(*, area_id: int, offset: int, start: datetime, end: datetime, raw: bool = False) -> dict:
    """Every S2 cell of one area: summary, plus its condition hour by hour."""
    cells = await _cell_rows([area_id], start, end)
    now = datetime.utcnow() + timedelta(hours=offset)
    out = {}
    for (_, cell), events in sorted(cells.items(), key=lambda kv: kv[0][1]):
        summary = defaultdict(float)
        hourly = defaultdict(lambda: defaultdict(float))
        for a, b, cond in _intervals(events, start, end, now):
            for hour, h in _hour_pieces(a, b):
                summary[cond] += h
                hourly[hour.strftime("%Y-%m-%d %H:00")][cond] += h
        if not summary:
            continue
        total = sum(summary.values())
        last = events[-1]
        out[str(cell)] = {
            "latitude": last[2],
            "longitude": last[3],
            "first_seen": events[0][0].strftime("%Y-%m-%d %H:%M"),
            "last_change": last[0].strftime("%Y-%m-%d %H:%M"),
            "changes": len(_change_times(events, start, end)),
            "summary": {_name(c, raw): {"hours": round(v, 2), "pct": round(100 * v / total, 1)}
                        for c, v in sorted(summary.items(), key=lambda kv: -kv[1])},
            "hourly": {h: (_name(next(iter(by)), raw) if len(by) == 1
                           else {_name(c, raw): round(v, 2) for c, v in by.items()})
                       for h, by in sorted(hourly.items())},
        }
    return {"cells": len(out), "data": out}
