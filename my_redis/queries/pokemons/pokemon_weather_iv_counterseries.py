from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.calc_iv_bucket import get_iv_bucket

redis_manager = RedisManager()

async def update_pokemon_weather_iv(data, pipe=None):
    """
    Increment Pokémon weather IV counters for the given area and IV range using Redis pipelines.

    Unique key is composed of:
      weather_boost + area + date
    and the field is the IV range bucket.

    Expected keys in `data`:
      - "first_seen": timestamp (in seconds)
      - "area_name": a string representing the area (e.g. "Saarlouirs")
      - "iv": the raw IV value (0-100)
      - "weather_boost": a flag (0 or 1, or a boolean; converted to int) indicating whether weather boost is active.
    """
    redis_status = await redis_manager.check_redis_connection("pokemon_pool")
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Pokémon weather IV counter.")
        return "ERROR"

    ts = data.get("first_seen")
    if ts is None:
        logger.error("❌ Missing 'first_seen' timestamp in data.")
        return "ERROR"
    # Use monthly timeframe
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m")

    area = data.get("area_name")
    if not area:
        logger.error("❌ Missing 'area_name' in data.")
        return "ERROR"

    raw_iv = data.get("iv")
    if raw_iv is None:
        logger.error("❌ Missing 'iv' value in data.")
        return "ERROR"

    iv_range = get_iv_bucket(raw_iv)
    if iv_range is None:
        logger.warning("⚠️ IV range conversion returned None; skipping update.")
        return "IGNORED"

    # Determine weather boost flag (0 or 1)
    weather_boost = int(bool(data.get("weather", 0)))

    # Construct Redis hash key. For example:
    # "counter:pokemon_weather_iv:Saarlouirs:20250310:1"
    hash_key = f"counter:pokemon_weather_iv:{area}:{date_str}:{weather_boost}"
    field_name = iv_range

    logger.debug(f"🔑 Constructed Hash Key: {hash_key}, Field: {field_name}")

    client = redis_manager.redis_client
    updated_fields = {}

    if pipe:
        pipe.hincrby(hash_key, field_name, 1)
        updated_fields[iv_range] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_name, 1)
            await pipe.execute()
        updated_fields[iv_range] = "OK"

    logger.info(f"✅ Incremented weather IV counter '{field_name}' for area '{area}' with weather boost {weather_boost}.")
    return updated_fields
