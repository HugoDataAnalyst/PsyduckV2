import time
from datetime import datetime, timedelta

from my_redis.queries.updates import count_once

DEFAULT_TTL_S = 3600   # disappear_time unknown


def stage_tappable_counters(data: dict, pipe) -> dict:
    """
    Count each tappable once, in its area's hourly, daily and weekly hashes.

    Keys:   counter:tappable_hourly:{area}:{YYYYMMDDHH}
            counter:tappable_daily:{area}:{YYYYMMDD}
            counter:tappable_total:{area}:{monday YYYYMMDD}
    Fields: {type}:{reward_type}:{reward_id}:total    +1 per tappable
            {type}:{reward_type}:{reward_id}:amount   +count (e.g. 6 candy)
    Marker: seen:tappable:{encounter_id}, kept until the tappable disappears.

    Bucketed by the (area-local) time Golbat recorded the reward.
    """
    area = data["area_name"]
    seen = datetime.fromtimestamp(data["seen_local"])
    monday = (seen - timedelta(days=seen.weekday())).strftime("%Y%m%d")
    base = f"{data['type']}:{data['reward_type']}:{data['reward_id']}"
    ttl = data["disappear_utc"] - time.time() if data["disappear_utc"] else DEFAULT_TTL_S

    increments = []
    for key in (f"counter:tappable_hourly:{area}:{seen.strftime('%Y%m%d%H')}",
                f"counter:tappable_daily:{area}:{seen.strftime('%Y%m%d')}",
                f"counter:tappable_total:{area}:{monday}"):
        increments += [(key, f"{base}:total", 1), (key, f"{base}:amount", data["count"])]
    count_once.stage(pipe, f"seen:tappable:{data['encounter_id']}", ttl, increments)
    return {"field": base}
