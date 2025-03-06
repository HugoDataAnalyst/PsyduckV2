from my_redis.connect_redis import redis_client
from utils.logger import logger

async def bulk_update_pokemon_stats(area_name, total, iv100, iv0):
    """Efficiently update multiple Redis keys using a pipeline transaction."""
    if not redis_client:
        logger.error("Redis is not connected.")
        return

    async with redis_client.pipeline(transaction=True) as pipe:
        await (pipe.hincrby(f"total_pokemon_stats:{area_name}", "total", total)
                  .hincrby(f"total_pokemon_stats:{area_name}", "iv100", iv100)
                  .hincrby(f"total_pokemon_stats:{area_name}", "iv0", iv0)
                  .execute())  # ✅ Atomic batch update

    logger.success(f"Updated stats for {area_name} in Redis.")
