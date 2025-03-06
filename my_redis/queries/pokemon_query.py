import asyncio
from my_redis.connect_redis import redis_client
from utils.logger import logger

async def listen_for_pokemon_updates():
    """Subscribe to real-time updates from `total_pokemon_stats`."""
    if not redis_client:
        logger.error("Redis is not connected.")
        return

    async with redis_client.pubsub() as pubsub:
        await pubsub.subscribe("total_pokemon_stats")  # ✅ Subscribe to updates
        logger.info("Subscribed to total_pokemon_stats updates.")

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message:
                logger.info(f"Received Pokémon stat update: {message['data'].decode()}")
            await asyncio.sleep(5)  # Avoid CPU overload
