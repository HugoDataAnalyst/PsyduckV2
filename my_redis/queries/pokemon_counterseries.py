from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_total_pokemon_counter(data):
    """
    Update daily counters for a Pokémon event.

    The keys are structured as:
      counter:pokemon:<metric>:<area>:<pokemon_id>:<form>:<YYYYMMDD>

    For each event, we increment the appropriate counter(s).
    """
    # Ensure Redis connection is active
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Pokémon counter.")
        return

    # Convert the first_seen timestamp (assumed to be in seconds) into a date string
    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")
    logger.debug(f"✅ Converted first_seen timestamp {ts} to date string {date_str}")

    area = data["area"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Define keys for each metric
    key_total      = f"counter:pokemon:total:{area}:{pokemon_id}:{form}:{date_str}"
    key_iv100      = f"counter:pokemon:iv100:{area}:{pokemon_id}:{form}:{date_str}"
    key_iv0        = f"counter:pokemon:iv0:{area}:{pokemon_id}:{form}:{date_str}"
    key_pvp_little = f"counter:pokemon:pvp_little:{area}:{pokemon_id}:{form}:{date_str}"
    key_pvp_great  = f"counter:pokemon:pvp_great:{area}:{pokemon_id}:{form}:{date_str}"
    key_pvp_ultra  = f"counter:pokemon:pvp_ultra:{area}:{pokemon_id}:{form}:{date_str}"

    logger.debug(f"🔑 Constructed counter keys: total={key_total}, iv100={key_iv100}, iv0={key_iv0}, "
                 f"pvp_little={key_pvp_little}, pvp_great={key_pvp_great}, pvp_ultra={key_pvp_ultra}")

    # Determine the metric values for this event
    total      = 1
    iv100      = 1 if data.get("iv") == 100 else 0
    iv0        = 1 if data.get("iv") == 0 else 0
    pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0

    logger.debug(f"🎚️ Counter metric values: total={total}, iv100={iv100}, iv0={iv0}, "
                 f"pvp_little={pvp_little}, pvp_great={pvp_great}, pvp_ultra={pvp_ultra}")

    client = redis_manager.redis_client

    # Use atomic increments to update the counters.
    await client.incrby(key_total, total)
    if iv100:
        await client.incrby(key_iv100, iv100)
    if iv0:
        await client.incrby(key_iv0, iv0)
    if pvp_little:
        await client.incrby(key_pvp_little, pvp_little)
    if pvp_great:
        await client.incrby(key_pvp_great, pvp_great)
    if pvp_ultra:
        await client.incrby(key_pvp_ultra, pvp_ultra)

    logger.info(f"✅ Updated Pokémon counters for Pokémon ID {pokemon_id} in area {area} on {date_str}")
    return {
        "total_key": key_total,
        "iv100_key": key_iv100,
        "iv0_key": key_iv0,
        "pvp_little_key": key_pvp_little,
        "pvp_great_key": key_pvp_great,
        "pvp_ultra_key": key_pvp_ultra,
        "date": date_str
    }
