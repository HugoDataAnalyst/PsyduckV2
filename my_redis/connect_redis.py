import redis.asyncio as redis
import config as AppConfig
from utils.logger import logger
import asyncio

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
            "redis_cleanup_pool": AppConfig.redis_cleanup_pool,
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
                client_name=pool_name,
            )
            client = redis.Redis(
                connection_pool=pool,
            )

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

    @staticmethod
    async def close_idle_clients(redis_client, max_idle_time=100):
        """
        Close Redis clients that have been idle for more than `max_idle_time` seconds.
        Returns a tuple: (closed_count, active_count, idle_but_young_count, report_by_name)
        where report_by_name is a dictionary with detailed stats per client name.
        """
        stats = {
            'closed': 0,
            'active': 0,
            'idle_but_young': 0,
            'current_connection': 0,
            'malformed': 0
        }

        # Dictionary to store detailed report by client name
        report_by_name = {}

        try:
            # Get current connection ID first to avoid killing ourselves
            current_conn_id = str(await redis_client.execute_command("CLIENT ID"))

            # Get client list
            clients = await redis_client.execute_command("CLIENT LIST")

            # Handle different response formats
            if isinstance(clients, str):
                clients = clients.splitlines()
            elif isinstance(clients, (list, tuple)):
                pass  # Already in correct format
            else:
                logger.error(f"❌ Unexpected CLIENT LIST format: {type(clients)}")
                return (0, 0, 0, {})

            for client in clients:
                try:
                    # Parse client info
                    if isinstance(client, str):
                        fields = dict(field.split("=", 1) for field in client.split(" "))
                    elif isinstance(client, dict):
                        fields = client
                    else:
                        stats['malformed'] += 1
                        continue

                    # Safely get values with defaults
                    client_id = fields.get("id", "").strip()
                    idle_time_str = fields.get("idle", "0").strip()
                    name = fields.get("name", "").strip() or "unnamed"
                    flags = fields.get("flags", "").strip()

                    # Skip if missing critical fields
                    if not client_id:
                        stats['malformed'] += 1
                        continue

                    # Track current connection
                    if client_id == current_conn_id:
                        stats['current_connection'] += 1
                        continue

                    # Convert idle time safely
                    try:
                        idle_time = int(idle_time_str)
                    except (ValueError, TypeError):
                        idle_time = 0

                    # Initialize report for this client name if not exists
                    if name not in report_by_name:
                        report_by_name[name] = {'active': 0, 'idle_but_young': 0, 'closed': 0, 'total': 0}
                    report_by_name[name]['total'] += 1

                    # Categorize clients and update counters
                    if idle_time == 0:
                        stats['active'] += 1
                        report_by_name[name]['active'] += 1
                        logger.debug(f"⚡ Active client {name} (ID: {client_id}, Flags: {flags})")
                    elif idle_time <= max_idle_time:
                        stats['idle_but_young'] += 1
                        report_by_name[name]['idle_but_young'] += 1
                        logger.debug(f"💤 Idle client {name} (ID: {client_id}, Idle: {idle_time}s/{max_idle_time}s)")
                    else:
                        stats['closed'] += 1
                        report_by_name[name]['closed'] += 1
                        logger.debug(f"❌ Closing idle client {name} (ID: {client_id}, Idle: {idle_time}s)")
                        await redis_client.execute_command(f"CLIENT KILL ID {client_id}")
                        await asyncio.sleep(0.05)  # Small delay between kills

                except Exception as e:
                    stats['malformed'] += 1
                    logger.warning(f"⏭️ Skipping client - processing error: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"❌ Client cleanup error: {e}", exc_info=True)
            return (0, 0, 0, {})

        return (stats['closed'], stats['active'], stats['idle_but_young'], report_by_name)

    @classmethod
    async def idle_client_cleanup(cls, redis_client, interval=60, max_idle_time=100):
        """
        Periodically check for and close idle Redis clients with detailed reporting.
        The detailed report (grouped by client name) is included in the success log.
        """
        logger.success(f"""
        🚀 Starting Redis Client Cleaner:
        • Interval: {interval}s
        • Max Idle: {max_idle_time}s
        • Next check in: {interval}s
        """.strip())

        while True:
            try:
                start_time = asyncio.get_event_loop().time()
                closed, active, idle_young, detailed_report = await cls.close_idle_clients(redis_client, max_idle_time)
                duration = round(asyncio.get_event_loop().time() - start_time, 3)

                # Sort the detailed report:
                # First by Total, then Active, then Idle but young, then Closed (all descending)
                sorted_report = sorted(
                    detailed_report.items(),
                    key=lambda item: (
                        item[1]['total'],
                        item[1]['active'],
                        item[1]['idle_but_young'],
                        item[1]['closed']
                    ),
                    reverse=True
                )

                detailed_str = "\n".join(
                    f"   - Name: {name} | Total: {counts['total']} | Active: {counts['active']} | Idle but young: {counts['idle_but_young']} | Closed: {counts['closed']}"
                    for name, counts in sorted_report
                )

                logger.success(f"""
                📊 Client Status Report:
                • 🔥 Active clients: {active}
                • 💤 Idle but young: {idle_young}
                • 🗑️ Closed idle clients: {closed}
                • ⏱️ Check duration: {duration}s
                • Next check in: {interval}s

                Detailed report by name:
                {detailed_str}
                """.strip())

            except Exception as e:
                logger.error(f"⚠️ Idle cleaner error: {str(e)}", exc_info=True)

            await asyncio.sleep(interval)
