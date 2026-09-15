import time
from typing import Any, Dict

from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.safe_values import _safe_int

redis_manager = RedisManager()

# Sorted set prefix for live (not yet despawned) Pokémon.
ACTIVE_KEY_PREFIX = "active:pokemon"

# Same metrics as ts:pokemon:* (minus shiny, which is account bound).
ACTIVE_METRICS = ("total", "iv100", "iv0", "pvp_little", "pvp_great", "pvp_ultra")

# Pokémon GO caps spawns at 60 minutes; the slack absorbs small clock skew.
MAX_DESPAWN_SECONDS = 3900
MIN_DESPAWN_SECONDS = 1


def build_active_key(area: str, metric: str) -> str:
    """
    Build the live sorted set key.

    Format:
      active:pokemon:Saarlouis:total
    """
    return f"{ACTIVE_KEY_PREFIX}:{area}:{metric}"


async def add_pokemon_active_event(data: Dict[str, Any], pipe=None) -> Dict[str, Any]:
    """
    Track a Pokémon as live until it despawns.

    Key format:
      active:pokemon:{area_name}:{metric}   (sorted set)

    The member is the encounter_id (so Golbat re-sends of the same encounter
    update in place instead of counting twice) and the score is the raw
    disappear_time epoch.

    Reads count with ZCOUNT from "now", so a despawned member is never counted
    even if the cleanup sweep has not removed it yet. Pruning is memory
    reclamation only, never a correctness requirement.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis connection failed")
        return {"status": "ERROR", "message": "Redis not connected"}

    area = data.get("area_name")
    if not area:
        logger.debug("⚠️ Skipping live Pokémon: missing area_name")
        return {"status": "IGNORED", "message": "Missing area_name"}

    despawn_epoch = _safe_int(data.get("disappear_time"), 0)
    if despawn_epoch <= 0:
        logger.debug(f"⚠️ Skipping live Pokémon in {area}: missing disappear_time")
        return {"status": "IGNORED", "message": "Missing disappear_time"}

    now = int(time.time())
    seconds_left = despawn_epoch - now

    if seconds_left < MIN_DESPAWN_SECONDS:
        logger.debug(f"⚠️ Skipping already despawned Pokémon in {area} ({seconds_left}s left)")
        return {"status": "IGNORED", "message": "Already despawned"}

    if seconds_left > MAX_DESPAWN_SECONDS:
        # Not a bad spawn: our clock and Golbat's are in different frames.
        logger.warning(
            f"⚠️ Refusing implausible disappear_time in {area}: {seconds_left}s left "
            f"(disappear_time={despawn_epoch}, now={now}). Check clock/timezone alignment."
        )
        return {"status": "IGNORED", "message": "Implausible disappear_time"}

    # Cross-check against the independently derived timer.
    despawn_timer = _safe_int(data.get("despawn_timer"), 0)
    if despawn_timer < MIN_DESPAWN_SECONDS or despawn_timer > MAX_DESPAWN_SECONDS:
        logger.debug(f"⚠️ Skipping live Pokémon in {area}: despawn_timer out of range ({despawn_timer}s)")
        return {"status": "IGNORED", "message": "Invalid despawn_timer"}

    if not data.get("disappear_time_verified", False):
        # Golbat's estimate rather than an observed despawn. Still live, still counted.
        logger.debug(f"🕒 Unverified disappear_time in {area} ({seconds_left}s left)")

    # Already canonicalised to an unsigned int64 string in filter_data.py.
    member = str(data.get("encounter_id") or "").strip()
    if not member:
        # Deterministic fallback so a re-send of the same spawn stays one member.
        # Keyed on first_seen, not disappear_time: first_seen is fixed for the
        # life of a spawn, while disappear_time gets revised when an estimated
        # despawn is later verified. Identity only - never compared to a clock.
        member = f"sp:{data.get('spawnpoint')}:{data.get('first_seen')}"

    pvp_little = data.get("pvp_little_rank")
    pvp_great = data.get("pvp_great_rank")
    pvp_ultra = data.get("pvp_ultra_rank")

    # Mirrors the metric logic in pokemon_timeseries.add_pokemon_timeseries_event.
    metrics = {
        "total": True,
        "iv100": data.get("iv") == 100,
        "iv0": data.get("iv") == 0,
        "pvp_little": bool(pvp_little) and 1 in pvp_little,
        "pvp_great": bool(pvp_great) and 1 in pvp_great,
        "pvp_ultra": bool(pvp_ultra) and 1 in pvp_ultra,
    }

    updated_fields = {}
    if pipe:
        for metric, applies in metrics.items():
            if applies:
                pipe.zadd(build_active_key(area, metric), {member: despawn_epoch})
                updated_fields[metric] = "OK"
    else:
        async with client.pipeline(transaction=False) as own_pipe:
            for metric, applies in metrics.items():
                if applies:
                    own_pipe.zadd(build_active_key(area, metric), {member: despawn_epoch})
                    updated_fields[metric] = "OK"
            await own_pipe.execute()

    logger.debug(f"✅ Live tracked {member} in {area} for {seconds_left}s ({len(updated_fields)} metrics)")
    return updated_fields
