import time

from my_redis.connect_redis import RedisManager
from my_redis.queries.updates.pokemons.pokemon_active import ACTIVE_METRICS, build_active_key
from utils.logger import logger

redis_manager = RedisManager()


class PokemonActiveRetrieval:
    """
    Live (not yet despawned) Pokémon counts for a single area.

    A Pokémon is live while now < disappear_time. Counts come straight from
    ZCOUNT over the despawn epoch score, so despawned members are never
    counted even when the cleanup sweep has not run yet - the read never
    needs to write.
    """

    def __init__(self, area: str, min_seconds_left: int = 0):
        self.area = area
        self.min_seconds_left = max(0, int(min_seconds_left))

    async def retrieve_active_counts(self) -> dict:
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection not available")
            return {"mode": "live", "data": {}}

        try:
            now = int(time.time())
            floor_score = now + self.min_seconds_left

            async with client.pipeline(transaction=False) as pipe:
                for metric in ACTIVE_METRICS:
                    # Exclusive floor: strictly more than min_seconds_left remaining.
                    # At min_seconds_left=0 this is exactly "now < disappear_time".
                    pipe.zcount(build_active_key(self.area, metric), f"({floor_score}", "+inf")
                raw_counts = await pipe.execute()

            counts = {metric: int(value or 0) for metric, value in zip(ACTIVE_METRICS, raw_counts)}
            logger.debug(f"✅ Live counts for {self.area} (>{self.min_seconds_left}s left): {counts}")

            return {
                "mode": "live",
                "data": {
                    "area": self.area,
                    "as_of": now,
                    "min_seconds_left": self.min_seconds_left,
                    "counts": counts,
                },
            }
        except Exception as e:
            logger.error(f"❌ Error retrieving live Pokémon counts for {self.area}: {e}")
            return {"mode": "live", "data": {}}
