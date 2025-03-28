import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def add_timeseries_invasion_event(data, pipe=None):
    """
    Add an Invasion event using plain text hash keys.

    Expected keys in `data`:
      - "invasion_first_seen": UTC timestamp (in seconds) for the invasion start
      - "area_name": area name (string)
      - "invasion_type": invasion display type (int)
      - "invasion_grunt_type": invasion grunt (int)
      - "invasion_confirmed": invasion confirmed flag (int or boolean; converted to 0 or 1)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot add Invasion event.")
        return "ERROR"

    invasion_first_seen = data["invasion_first_seen"]
    bucket = str((invasion_first_seen // 60) * 60)  # Round to nearest minute (seconds)

    area = data["area_name"]
    display_type = data["invasion_type"]
    grunt = data["invasion_grunt_type"]
    confirmed = 1 if data["invasion_confirmed"] else 0

    # Construct key in the format:
    # ts:invasion:total:{area}:{invasion_type}:{grunt}:{confirmed}
    key_total = f"ts:invasion:total:{area}:{display_type}:{grunt}:{confirmed}"
    logger.debug(f"🔑 Constructed Invasion key: {key_total}")

    inc_total = 1  # Always add 1 for total.
    updated_fields = {}

    if pipe:
        pipe.hincrby(key_total, bucket, inc_total)
        updated_fields["total"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(key_total, bucket, inc_total)
            updated_fields["total"] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added Invasion event for display {display_type} with grunt {grunt} in area {area}")
    return updated_fields
