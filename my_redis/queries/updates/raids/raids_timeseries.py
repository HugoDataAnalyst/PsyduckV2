import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def add_raid_timeseries_event(data, pipe=None):
    """
    Add a Raid event using plain text hash keys.

    Expected keys in `data`:
      - "raid_first_seen": raid spawn UTC timestamp (in seconds)
      - "area_name": area name (string)
      - "raid_pokemon": raid boss Pokémon ID (integer)
      - "raid_level": raid level (integer)
      - "raid_form": raid boss form (integer, default 0)
      - "raid_costume": raid boss costume (integer)
      - "raid_is_exclusive": whether the raid is exclusive (boolean/int; converted to 0 or 1)
      - "raid_ex_raid_eligible": whether the raid is eligible for EX (boolean/int; converted to 0 or 1)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot add Raid event.")
        return "ERROR"

    # Round raid_first_seen to nearest minute (in seconds).
    raid_first_seen = data["raid_first_seen"]
    bucket = str((raid_first_seen // 60) * 60)

    area = data["area_name"]
    raid_pokemon = data["raid_pokemon"]
    raid_level = data["raid_level"]
    raid_form = data["raid_form"]
    raid_costume = data["raid_costume"]
    raid_is_exclusive = data["raid_is_exclusive"]
    raid_ex_raid_eligible = data["raid_ex_raid_eligible"]

    # Build keys using a human‑readable format.
    key_total = f"ts:raids_total:total:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
    key_costume = f"ts:raids_total:costume:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
    key_exclusive = f"ts:raids_total:exclusive:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
    key_ex_raid_eligible = f"ts:raids_total:ex_raid_eligible:{area}:{raid_pokemon}:{raid_level}:{raid_form}"

    logger.debug(f"🔑 Constructed Raid key: {key_total}")

    # Determine increments.
    inc_total = 1  # Always add 1 for total.
    inc_costume = int(bool(raid_costume) and str(raid_costume) != "0")
    inc_exclusive = 1 if raid_is_exclusive == 1 else 0
    inc_ex_raid_eligible = 1 if raid_ex_raid_eligible == 1 else 0

    updated_fields = {}
    if pipe:
        pipe.hincrby(key_total, bucket, inc_total)
        updated_fields["total"] = "OK"
        if inc_costume:
            pipe.hincrby(key_costume, bucket, inc_costume)
            updated_fields["costume"] = "OK"
        if inc_exclusive:
            pipe.hincrby(key_exclusive, bucket, inc_exclusive)
            updated_fields["exclusive"] = "OK"
        if inc_ex_raid_eligible:
            pipe.hincrby(key_ex_raid_eligible, bucket, inc_ex_raid_eligible)
            updated_fields["ex_raid_eligible"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(key_total, bucket, inc_total)
            updated_fields["total"] = "OK"
            if inc_costume:
                pipe.hincrby(key_costume, bucket, inc_costume)
                updated_fields["costume"] = "OK"
            if inc_exclusive:
                pipe.hincrby(key_exclusive, bucket, inc_exclusive)
                updated_fields["exclusive"] = "OK"
            if inc_ex_raid_eligible:
                pipe.hincrby(key_ex_raid_eligible, bucket, inc_ex_raid_eligible)
                updated_fields["ex_raid_eligible"] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added Raid event for raid_pokemon {raid_pokemon} in area {area}")
    return updated_fields
