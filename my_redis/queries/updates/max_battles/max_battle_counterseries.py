import time
from datetime import datetime, timedelta

from my_redis.queries.updates import count_once


def stage_max_battle_counters(data: dict, pipe) -> dict:
    """
    Count each max battle of a station once, in its area's daily and weekly hashes.

    Keys:   counter:max_battle_daily:{area}:{YYYYMMDD}
            counter:max_battle_total:{area}:{monday YYYYMMDD}
    Field:  {battle_pokemon_id}:{level}:{form}:{costume}:{bread_mode}:total
    Marker: seen:max_battle:{station}:{bread_battle_seed}, kept until the battle ends.

    Battles are bucketed by their (area-local) battle_start.
    """
    area = data["area_name"]
    station = data["station_id"]
    now = time.time()
    staged = 0
    for b in data["battles"]:
        start = datetime.fromtimestamp(b["start_local"])
        monday = (start - timedelta(days=start.weekday())).strftime("%Y%m%d")
        field = f"{b['pokemon_id']}:{b['level']}:{b['form']}:{b['costume']}:{b['bread_mode']}:total"
        count_once.stage(
            pipe,
            f"seen:max_battle:{station}:{b['seed']}",
            b["end_utc"] - now,
            [
                (f"counter:max_battle_daily:{area}:{start.strftime('%Y%m%d')}", field, 1),
                (f"counter:max_battle_total:{area}:{monday}", field, 1),
            ],
        )
        staged += 1
    return {"battles": staged}
