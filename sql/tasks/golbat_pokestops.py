import time
import json
import asyncio
import aiomysql
import config as AppConfig
from utils.logger import logger
from server_fastapi import global_state
from utils.koji_geofences import KojiGeofences
from my_redis.connect_redis import RedisManager

async def get_golbat_mysql_pool():
    """
    Create and return an aiomysql connection pool for the Golbat DB.
    """
    pool = await aiomysql.create_pool(
        host=AppConfig.golbat_db_host,
        port=AppConfig.golbat_db_port,
        user=AppConfig.golbat_db_user,
        password=AppConfig.golbat_db_password,
        db=AppConfig.golbat_db_name,
        autocommit=True,
        loop=asyncio.get_running_loop()
    )
    return pool

class GolbatSQLPokestops:
    redis_manager = RedisManager()
    koji_instance = KojiGeofences(AppConfig.geofence_refresh_cache_seconds)
    cache_key = "cached_pokestops"
    cache_expiry = AppConfig.pokestop_cache_expiry_seconds if hasattr(AppConfig, "pokestop_cache_expiry_seconds") else 300
    logger.info(f"Set pokestops cache to: {cache_expiry} seconds")
    @classmethod
    async def refresh_pokestops(cls):
        """
        Query MySQL to count the number of pokestops within each cached geofence and
        store the counts in Redis. Results are saved in the following format:
            {
              "areas": { "AreaName1": count1, "AreaName2": count2, ... },
              "grand_total": total_count
            }
        """
        max_retries = 5
        try:
            # Retrieve cached geofences from Koji
            geofences = await cls.koji_instance.get_cached_geofences()
            if not geofences:
                logger.warning("⚠️ No geofences retrieved; skipping pokestop refresh")
                return

            pool = await get_golbat_mysql_pool()
            area_counts = {}
            grand_total = 0

            for geofence in geofences:
                area_name = geofence.get("name")
                coordinates = geofence.get("coordinates", [])
                if not coordinates or not coordinates[0]:
                    logger.warning(f"⚠️ No coordinates for area '{area_name}', skipping")
                    continue

                # Build the polygon WKT string
                try:
                    poly_coords = coordinates[0]
                    coord_str = ", ".join(f"{lon} {lat}" for lon, lat in poly_coords)
                    polygon_wkt = f"POLYGON(({coord_str}))"
                except Exception as ex:
                    logger.error(f"❌ Failed to build polygon for area '{area_name}': {ex}")
                    continue


                retries = 0
                count = 0
                while retries < max_retries:
                    try:
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cur:
                                sql = """
                                SELECT COUNT(*) AS cnt FROM pokestop
                                WHERE ST_CONTAINS(ST_GeomFromText(%s), POINT(lon, lat));
                                """
                                await cur.execute(sql, (polygon_wkt,))
                                result = await cur.fetchone()
                                count = result[0] if result else 0
                        logger.info(f"🏙️ Area '{area_name}' has {count} pokestops inside its geofence.")
                        break

                    except Exception as ex:
                        retries += 1
                        logger.error(f"❌ Error retrieving data for area '{area_name}' (attempt {retries}/{max_retries}): {ex}")
                        if retries < max_retries:
                            logger.info(f"🔄 Retrying area '{area_name}'...")
                            await asyncio.sleep(1)
                        else:
                            logger.error(f"⚠️ Max retries exceeded for area '{area_name}'. Skipping.")

                area_counts[area_name] = count
                grand_total += count

            pool.close()
            await pool.wait_closed()

            # Build the final result dictionary.
            final_data = {"areas": area_counts, "grand_total": grand_total}

            # Cache the result in Redis.
            redis_client = await cls.redis_manager.check_redis_connection()
            if redis_client:
                await redis_client.set(cls.cache_key, json.dumps(final_data), ex=cls.cache_expiry)
                logger.success(f"✅ Cached pokestop counts: {final_data}")
            else:
                logger.error("❌ Redis not connected; could not cache pokestop counts.")

            # Optionally update global_state
            global_state.cached_pokestops = final_data

            return final_data

        except Exception as e:
            logger.error(f"❌ Error refreshing pokestop counts: {e}", exc_info=True)
            return None

    @classmethod
    async def get_cached_pokestops(cls):
        """
        Retrieve cached pokestop counts from Redis.
        """
        redis_client = await cls.redis_manager.check_redis_connection()
        if not redis_client:
            logger.error("❌ Redis not connected; cannot retrieve cached pokestops.")
            return None

        cached = await redis_client.get(cls.cache_key)
        if cached:
            try:
                result = json.loads(cached)
                logger.success(f"✅ Retrieved cached pokestops: {result}")
                return result
            except Exception as ex:
                logger.error(f"❌ Failed to parse cached pokestops: {ex}")
                return None
        else:
            logger.warning("⚠️ No cached pokestops found. Triggering 🔃 refresh!")
            result = await cls.refresh_pokestops()
            if result is None:
                logger.warning("⚠️ Failed to refresh pokestops. Trying to cache global 🌐 state.")
                if global_state.cached_pokestops:
                    try:
                        redis_client = await cls.redis_manager.check_redis_connection()
                        await redis_client.set(cls.cache_key, json.dumps(global_state.cached_pokestops), ex=cls.cache_expiry)
                        logger.success("✅ Cached 🌐 global_state.cached_pokestops as fallback.")
                    except Exception as ex:
                        logger.error(f"❌ Failed to cache 🌐 global_state.cached_pokestops: {ex}")
                else:
                    logger.warning("⚠️ No 🌐 global_state.cached_pokestops available as fallback.")
                return global_state.cached_pokestops
            return result

    @classmethod
    async def run_refresh_loop(cls, refresh_interval: int):
        """
        Continuously refresh the pokestop counts at the given interval (in seconds).
        """
        while True:
            logger.info("🔃 Refreshing pokestop counts...")
            await cls.refresh_pokestops()
            logger.info(f"⏰ Next pokestop refresh in {refresh_interval} seconds.")
            await asyncio.sleep(refresh_interval)

