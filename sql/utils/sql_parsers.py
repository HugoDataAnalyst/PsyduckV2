from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import date

def _build_in_clause(col: str, values: List[Any] | None) -> Tuple[str, List[Any]]:
    if not values:
        return "", []
    placeholders = ", ".join(["%s"] * len(values))
    return f"{col} IN ({placeholders})", list(values)

def _csv_to_int_list(s: str) -> Optional[List[int]]:
    s = (s or "").strip().lower()
    if s in ("", "all", "global"):
        return None
    return [int(x) for x in s.split(",") if x.strip()]

def _csv_to_str_list(s: str) -> Optional[List[str]]:
    s = (s or "").strip().lower()
    if s in ("", "all", "global"):
        return None
    return [x.strip() for x in s.split(",") if x.strip()]

def _iter_months_inclusive(start_first_of_month: date, end_first_of_month: date):
    """Yield (month_year_int, y, m) inclusive, with month_year_int in YYMM (e.g., 2509 for 2025-09)."""
    y, m = start_first_of_month.year, start_first_of_month.month
    y2, m2 = end_first_of_month.year, end_first_of_month.month

    def _yymm(year: int, month: int) -> int:
        return (year % 100) * 100 + month  # 2025-09 -> 25*100+9 = 2509

    while (y < y2) or (y == y2 and m <= m2):
        yield _yymm(y, m), y, m
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
