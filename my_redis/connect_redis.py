from socket import timeout
import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger

class RedisManager:
    """Manages separate Redis connection pools for each webhook type."""
    redis_url = AppConfig.redis_url
    _instances = {}  # ✅ Dictionary to store different pools

    @classmethod
    def get_max_connections_for_pool(cls, pool_name):
        """Return the max connections based on the pool type."""
        pool_config = {
            "pokemon_pool": AppConfig.redis_pokemon_pool,
            "quest_pool": AppConfig.redis_quest_pool,
            "raid_pool": AppConfig.redis_raid_pool,
            "invasion_pool": AppConfig.redis_invasion_pool,
            "koji_geofence_pool": AppConfig.redis_geofence_pool,
            "retrieval_pool": AppConfig.redis_retrieval_pool,
            "flush_heatmap_pool": AppConfig.redis_heatmap_pool,
            "flush_shiny_pool": AppConfig.redis_shiny_pool,
            "sql_pokemon_pool": AppConfig.redis_sql_pokemon_pool,
        }
        max_conn = pool_config.get(pool_name, 5)  # Default to 5 connections if not found
        logger.success(f"🔎 Using {max_conn} connections for {pool_name} pool.")
        return max_conn

    @classmethod
    async def init_pool(cls, pool_name, max_connections=10):
        """Initialize a separate Redis pool per webhook type."""
        if pool_name in cls._instances:
            return cls._instances[pool_name]  # ✅ Return existing pool

        try:
            logger.info(f"🔃 Connecting Redis ({pool_name}) with a pool of {max_connections} connections...")
            pool = redis.ConnectionPool.from_url(
                cls.redis_url,
                max_connections=max_connections,
                encoding="utf-8",
                decode_responses=True,
                timeout=30,
                max_idle_time=30
            )
            client = redis.Redis(connection_pool=pool)

            if await client.ping():
                cls._instances[pool_name] = client  # Store the connection pool
                logger.success(f"✅ Redis ({pool_name}) connected!")
                return client
            else:
                logger.error(f"❌ Redis ({pool_name}) failed to connect!")
                return None
        except Exception as e:
            logger.error(f"❌ Redis ({pool_name}) connection error: {e}")
            return None

    @classmethod
    async def close_all_pools(cls):
        """Close all Redis connection pools."""
        for pool_name, client in cls._instances.items():
            await client.close()
            logger.success(f"✅ Closed Redis connection pool: ({pool_name})")
        cls._instances.clear()

    @classmethod
    async def check_redis_connection(cls, pool_name):
        """Check if a Redis connection is active for a specific pool."""
        client = cls._instances.get(pool_name)

        if client:
            try:
                if await client.ping():  # ✅ Check if Redis connection is alive
                    logger.debug(f"✅ Redis ({pool_name}) is active.")
                    return client
                else:
                    logger.warning(f"⚠️ Redis ({pool_name}) pool exists but is unresponsive. Reconnecting...")
            except Exception as e:
                logger.warning(f"⚠️ Redis ({pool_name}) connection lost: {e}. Reconnecting...")

        # ❌ Either the pool didn't exist or was unresponsive, so reinitialize it with the correct max_connections
        max_connections = cls.get_max_connections_for_pool(pool_name)
        return await cls.init_pool(pool_name, max_connections=max_connections)


    @classmethod
    async def get_client(cls, pool_name):
        """Get a Redis client for the specified pool."""
        return await cls.check_redis_connection(pool_name)
