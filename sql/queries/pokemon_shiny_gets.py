import asyncio
from datetime import date
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from sql.utils.sql_parsers import _build_in_clause, _iter_months_inclusive, _csv_to_int_list, _csv_to_str_list
from sql.connect_db import fetch_all
from sql.utils.area_parser import resolve_area_id_by_name

# single-month CTE

async def _fetch_species_single_month(
    month_year_int: int,
    area_id: Optional[int],
    usernames: Optional[List[str]],
    pid_list: Optional[List[int]],
    form_list: Optional[List[str]],
    min_user_n: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """

    WITH per_user AS (
      SELECT pokemon_id, form, username,
             SUM(total_count) AS n,
             SUM(CASE WHEN shiny=1 THEN total_count ELSE 0 END) AS shiny_n
      FROM shiny_username_rates
      WHERE month_year=? [AND ...filters...]
      GROUP BY pokemon_id, form, username
    )
    SELECT pokemon_id, form,
           ROUND(100*AVG(shiny_n/NULLIF(n,0)),4) AS shiny_pct_macro,
           ROUND(100*SUM(shiny_n)/NULLIF(SUM(n),0),4) AS shiny_pct_pooled,
           COUNT(*) AS users_contributing,
           SUM(n)   AS total_encounters
    FROM per_user
    WHERE n >= ?
    GROUP BY pokemon_id, form
    ORDER BY pokemon_id, form, shiny_pct_macro DESC
    """
    base_where = ["month_year = %s"]
    params: List[Any] = [month_year_int]

    if area_id is not None:
        base_where.append("area_id = %s")
        params.append(area_id)

    # username IN (...)
    if usernames:
        u_sql, u_vals = _build_in_clause("username", usernames)
        base_where.append(u_sql); params += u_vals

    # pokemon IN (...)
    p_sql, p_vals = _build_in_clause("pokemon_id", pid_list)
    if p_sql:
        base_where.append(p_sql); params += p_vals

    # form IN (...)
    f_sql, f_vals = _build_in_clause("form", form_list)
    if f_sql:
        base_where.append(f_sql); params += f_vals

    where_sql = " AND ".join(base_where)
    where_min_sql = "WHERE pu.n >= %s" if (min_user_n and min_user_n > 0) else ""
    if min_user_n and min_user_n > 0:
        params.append(int(min_user_n))

    limit_sql = f" LIMIT {int(limit)}" if (limit and limit > 0) else ""

    sql = f"""
        SELECT
          pu.pokemon_id,
          pu.form,
          ROUND(100 * AVG(pu.shiny_n / NULLIF(pu.n,0)), 4) AS shiny_pct_macro,
          ROUND(100 * SUM(pu.shiny_n) / NULLIF(SUM(pu.n),0), 4) AS shiny_pct_pooled,
          COUNT(*) AS users_contributing,
          SUM(pu.n) AS total_encounters
        FROM (
          SELECT
            pokemon_id,
            form,
            username,
            SUM(total_count) AS n,
            SUM(CASE WHEN shiny = 1 THEN total_count ELSE 0 END) AS shiny_n
          FROM shiny_username_rates
          WHERE {where_sql}
          GROUP BY pokemon_id, form, username
        ) pu
        {where_min_sql}
        GROUP BY pu.pokemon_id, pu.form
        ORDER BY pu.pokemon_id ASC, pu.form ASC, shiny_pct_macro DESC
        {limit_sql}
    """
    return await fetch_all(sql, params)

# per-month / per-user fetch

async def _fetch_per_user_month(
    month_year_int: int,
    area_id: Optional[int],
    usernames: Optional[List[str]],
    pid_list: Optional[List[int]],
    form_list: Optional[List[str]],
) -> List[Dict[str, Any]]:
    """
    Returns rows at per-user granularity for this month:
      {pokemon_id, form, username, n, shiny_n}
    """
    where = ["month_year = %s"]
    params: List[Any] = [month_year_int]

    if area_id is not None:
        where.append("area_id = %s")
        params.append(area_id)

    if usernames:
        u_sql, u_vals = _build_in_clause("username", usernames)
        where.append(u_sql); params += u_vals

    p_sql, p_vals = _build_in_clause("pokemon_id", pid_list)
    if p_sql:
        where.append(p_sql); params += p_vals

    f_sql, f_vals = _build_in_clause("form", form_list)
    if f_sql:
        where.append(f_sql); params += f_vals

    where_sql = " AND ".join(where)

    sql = f"""
        SELECT
          pokemon_id,
          form,
          username,
          SUM(total_count) AS n,
          SUM(CASE WHEN shiny = 1 THEN total_count ELSE 0 END) AS shiny_n
        FROM shiny_username_rates
        WHERE {where_sql}
        GROUP BY pokemon_id, form, username
    """
    return await fetch_all(sql, params)

# range orchestrator

async def fetch_shiny_rates_range(
    start_month_date: date,
    end_month_date: date,
    area_name: str,
    usernames_csv: Optional[str],   # CSV or None
    pokemon_id: str,                # CSV or 'all'
    form: str,                      # CSV or 'all'
    min_user_n: int = 0,
    limit: int = 0,
    concurrency: int = 4,
) -> Dict[str, Any]:
    # resolve area allow all/global
    area_id: Optional[int] = None
    if (area_name or "").strip().lower() not in ("", "all", "global"):
        area_id = int(await resolve_area_id_by_name(area_name))

    # Parse filters
    pid_list  = _csv_to_int_list(pokemon_id)
    form_list = _csv_to_str_list(form)
    usernames = _csv_to_str_list(usernames_csv or "all")  # None for 'all'

    months = list(_iter_months_inclusive(start_month_date, end_month_date))
    single_month = (len(months) == 1)

    if single_month:
        # Run the exact CTE logic server-side
        myi = months[0][0]
        out_rows = await _fetch_species_single_month(
            month_year_int=myi,
            area_id=area_id,
            usernames=usernames,
            pid_list=pid_list,
            form_list=form_list,
            min_user_n=int(min_user_n or 0),
            limit=int(limit or 0),
        )
        start_myi = end_myi = myi
        rows = out_rows
    else:
        # Multi-month: fetch per-user per-month, merge per-user across months,
        # then apply per-user min and aggregate to species.
        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def _task(myi: int):
            async with sem:
                return await _fetch_per_user_month(
                    month_year_int=myi,
                    area_id=area_id,
                    usernames=usernames,
                    pid_list=pid_list,
                    form_list=form_list,
                )

        per_month_lists = await asyncio.gather(
            *[asyncio.create_task(_task(myi)) for (myi, _, _) in months]
        )

        # Merge across months by pid, form, username
        per_user_agg: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
        for rows_m in per_month_lists:
            for r in rows_m:
                key = (int(r["pokemon_id"]), str(r["form"]), str(r["username"]))
                e = per_user_agg.get(key)
                if e is None:
                    per_user_agg[key] = {"n": int(r["n"] or 0), "shiny_n": int(r["shiny_n"] or 0)}
                else:
                    e["n"] += int(r["n"] or 0)
                    e["shiny_n"] += int(r["shiny_n"] or 0)

        # Apply per-user minimum AFTER month merge
        if min_user_n and min_user_n > 0:
            per_user_agg = {k: v for k, v in per_user_agg.items() if v["n"] >= int(min_user_n)}

        # Aggregate to pid, form
        by_species: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for (pid, form_s, _user), v in per_user_agg.items():
            rate = (v["shiny_n"] / v["n"]) if v["n"] > 0 else 0.0
            sp = by_species.get((pid, form_s))
            if sp is None:
                by_species[(pid, form_s)] = {
                    "pokemon_id": pid,
                    "form": form_s,
                    "users_contributing": 1,
                    "total_encounters": v["n"],
                    "_macro_sum": rate,
                    "_macro_cnt": 1,
                    "_pool_shiny": v["shiny_n"],
                    "_pool_total": v["n"],
                }
            else:
                sp["users_contributing"] += 1
                sp["total_encounters"] += v["n"]
                sp["_macro_sum"] += rate
                sp["_macro_cnt"] += 1
                sp["_pool_shiny"] += v["shiny_n"]
                sp["_pool_total"] += v["n"]

        rows = []
        for sp in by_species.values():
            macro = (sp["_macro_sum"] / sp["_macro_cnt"]) if sp["_macro_cnt"] > 0 else 0.0
            pooled = (sp["_pool_shiny"] / sp["_pool_total"]) if sp["_pool_total"] > 0 else 0.0
            rows.append({
                "pokemon_id": sp["pokemon_id"],
                "form": sp["form"],
                "shiny_pct_macro": round(100.0 * macro, 4),
                "shiny_pct_pooled": round(100.0 * pooled, 4),
                "users_contributing": sp["users_contributing"],
                "total_encounters": sp["total_encounters"],
            })

        rows.sort(key=lambda r: (r["pokemon_id"], r["form"], -r["shiny_pct_macro"]))
        if limit and limit > 0:
            rows = rows[: int(limit)]

        start_myi = months[0][0]
        end_myi   = months[-1][0]

    # Uniform response
    return {
        "start_month": int(start_myi),
        "end_month": int(end_myi),
        "area": area_name,
        "filters": {
            "usernames": usernames,
            "pokemon_id": pid_list,
            "form": form_list,
            "min_user_n": int(min_user_n),
        },
        "rows": len(rows),
        "data": rows,
    }
