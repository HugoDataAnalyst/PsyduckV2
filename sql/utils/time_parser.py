import re
from utils.logger import logger
from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta

def month_parse_time_input(value: str) -> date:
    """
    Parse month-like inputs to a date pinned to the 1st of that month.
    Accepts: 'YYYYMM', 'YYYY-MM', 'YYYYMMDD', 'YYYY-MM-DD', ISO timestamp.
    """
    s = (value or "").strip()
    if not s:
        raise ValueError("Empty time value")

    # ISO timestamp or ISO date
    try:
        dt = datetime.fromisoformat(s)
        return date(dt.year, dt.month, 1)
    except Exception:
        pass

    s_clean = s.replace("-", "")
    if len(s_clean) == 6 and s_clean.isdigit():  # YYYYMM
        y, m = int(s_clean[:4]), int(s_clean[4:6])
        return date(y, m, 1)
    if len(s_clean) == 8 and s_clean.isdigit():  # YYYYMMDD
        y, m = int(s_clean[:4]), int(s_clean[4:6])
        return date(y, m, 1)

    raise ValueError("Invalid month format. Use YYYYMM, YYYY-MM, YYYYMMDD, YYYY-MM-DD, or ISO timestamp.")


def parse_time_input(value: str) -> date:
    """
    Parse day-like inputs to a date (no timezone math here; we floor to local date).
    Accepts:
      - ISO date/timestamp: 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM[:SS[.fff]]'
      - 'YYYYMMDD' or 'YYYY-MM-DD'
      - 'YYYYMM' or 'YYYY-MM' (coerced to 1st of month)
      - Keywords: 'now', 'today', 'yesterday'
      - Relative: '<n> day(s)|week(s)|month(s)|year(s)'
    """
    s = (value or "").strip().lower()
    if not s:
        raise ValueError("Empty time value")

    # Keywords
    today = datetime.utcnow().date()
    if s in ("now", "today"):
        return today
    if s == "yesterday":
        return today - timedelta(days=1)

    # ISO timestamp/date
    try:
        dt = datetime.fromisoformat(value)
        return dt.date()
    except Exception:
        pass

    # Numeric forms
    s_clean = s.replace("-", "")
    if len(s_clean) == 8 and s_clean.isdigit():  # YYYYMMDD
        y, m, d = int(s_clean[:4]), int(s_clean[4:6]), int(s_clean[6:8])
        return date(y, m, d)
    if len(s_clean) == 6 and s_clean.isdigit():  # YYYYMM > 1st of month
        y, m = int(s_clean[:4]), int(s_clean[4:6])
        return date(y, m, 1)

    # Relative forms
    m = re.fullmatch(r"(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)", s)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        now = datetime.utcnow()
        if unit.startswith("second"):
            past = now - timedelta(seconds=n)
        elif unit.startswith("minute"):
            past = now - timedelta(minutes=n)
        elif unit.startswith("hour"):
            past = now - timedelta(hours=n)
        elif unit.startswith("day"):
            past = now - timedelta(days=n)
        elif unit.startswith("week"):
            past = now - timedelta(weeks=n)
        elif unit.startswith("month"):
            past = now - relativedelta(months=n)
        else:  # year/years
            past = now - relativedelta(years=n)
        return past.date()

    raise ValueError("Invalid date format. Use ISO date/timestamp, YYYYMMDD/YYY-MM-DD, YYYYMM/YYY-MM, or a relative like '10 days'.")

def parse_time_to_datetime(value: str) -> datetime:
    """
    Parse into a naive UTC datetime.
    Accepts:
      - ISO date/timestamp: 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM[:SS[.fff]]'
      - 'now' -> current UTC datetime
      - 'today' -> today 00:00:00 UTC
      - 'yesterday' -> yesterday 00:00:00 UTC
      - Numeric dates:
          * 'YYYYMMDD' -> YYYY-MM-DD 00:00:00
          * 'YYYYMM'   -> YYYY-MM-01 00:00:00
      - Relative: '<n> second(s)|minute(s)|hour(s)|day(s)|week(s)|month(s)|year(s)'
        Returns now - delta at datetime precision.
    """
    s = (value or "").strip().lower()
    if not s:
        raise ValueError("Empty time value")

    if s == "now":
        return datetime.utcnow()
    if s == "today":
        t = datetime.utcnow().date()
        return datetime(t.year, t.month, t.day)
    if s == "yesterday":
        t = (datetime.utcnow().date() - timedelta(days=1))
        return datetime(t.year, t.month, t.day)

    # ISO timestamp/date
    try:
        dt = datetime.fromisoformat(value.replace("Z", ""))
        # If only a date part was provided, dt will be 00:00 time already
        return dt
    except Exception:
        pass

    # Numeric forms
    s_clean = s.replace("-", "")
    if len(s_clean) == 8 and s_clean.isdigit():  # YYYYMMDD
        y, m, d = int(s_clean[:4]), int(s_clean[4:6]), int(s_clean[6:8])
        return datetime(y, m, d, 0, 0, 0)
    if len(s_clean) == 6 and s_clean.isdigit():  # YYYYMM > 1st of month
        y, m = int(s_clean[:4]), int(s_clean[4:6])
        return datetime(y, m, 1, 0, 0, 0)

    # Relative forms
    m = re.fullmatch(r"(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)", s)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        now = datetime.utcnow()
        if unit.startswith("second"):
            return now - timedelta(seconds=n)
        elif unit.startswith("minute"):
            return now - timedelta(minutes=n)
        elif unit.startswith("hour"):
            return now - timedelta(hours=n)
        elif unit.startswith("day"):
            return now - timedelta(days=n)
        elif unit.startswith("week"):
            return now - timedelta(weeks=n)
        elif unit.startswith("month"):
            return now - relativedelta(months=n)
        else:  # year/years
            return now - relativedelta(years=n)

    raise ValueError("Invalid datetime format. Use ISO, 'now', 'today', 'yesterday', numeric dates, or a relative like '10 hours'.")


def daterange_inclusive_days(start_dt: datetime, end_dt: datetime) -> list[date]:
    """Return [start.date() .. end.date()] inclusive."""
    start_day = start_dt.date()
    end_day = end_dt.date()
    n = (end_day - start_day).days
    return [start_day + timedelta(days=i) for i in range(n + 1)]

def clip_seen_window_for_day(day: date, seen_from: datetime | None, seen_to: datetime | None):
    """Intersect [seen_from, seen_to] with this day's [00:00:00, 23:59:59]; return (start, end) or None."""
    if not seen_from and not seen_to:
        return None
    day_start = datetime.combine(day, time.min)
    day_end   = datetime.combine(day, time.max).replace(microsecond=0)
    start = max(day_start, seen_from) if seen_from else day_start
    end   = min(day_end,   seen_to)   if seen_to   else day_end
    if start > end:
        return None
    return start, end
