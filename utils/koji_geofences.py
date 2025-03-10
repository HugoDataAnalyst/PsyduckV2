import asyncio
import json
import httpx
import redis
from shapely import area
import config as AppConfig
from datetime import datetime, timedelta
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from sql.models import AreaNames
from server_fastapi import global_state

class KojiGeofences:
    """Koji Geofences class.
    Handles fetching, caching and retrieving Koji Geofences from its API.
    """
    # Start the class static variables
    geofence_key = "koji_geofences"
    expiry = AppConfig.geofence_expire_cache_seconds
    bearer_token = AppConfig.koji_bearer_token
    geofence_api_url = AppConfig.koji_geofence_api_url
    redis_manager = RedisManager() # RedisManager is a Singleton
    _instance = None # ✅ Singleton instance

    def __new__(cls, refresh_interval):
        """Ensures only one KojiGeofences instance is created globally (Singleton)."""
        if cls._instance is None:
            cls._instance = super(KojiGeofences, cls).__new__(cls)
            cls._instance.refresh_interval = refresh_interval
        return cls._instance


    @classmethod
    async def get_koji_geofences(cls):
        """Fetch Koji Geofences from the API and extract relevant details."""
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {cls.bearer_token}"}
            response = await client.get(cls.geofence_api_url, headers=headers)

            if response.status_code == 200:
                raw_data = response.json().get("data", {}).get("features", [])
                geofences = []

                for feature in raw_data:
                    properties = feature.get("properties", {})
                    geometry = feature.get("geometry", {})

                    if geometry.get("type") == "Polygon":
                        # Use the area name from the API response
                        area_name = properties.get("name", "Unknown")
                        # Check if the area exists; if not insert it
                        area_obj, created = await AreaNames.get_or_create(name=area_name)
                        # Append the area object to the list of geofences
                        if created:
                            logger.debug(f"✅ Created new area: {area_name}")
                        else:
                            logger.debug(f"Found existing area: '{area_name}' with id {area_obj.id}")

                        geofences.append({
                            "id": area_obj.id,
                            "name": area_name,
                            "coordinates": geometry.get("coordinates", [])
                        })

                logger.debug(f"✅ Parsed {len(geofences)} Koji Geofences.")
                return geofences

            else:
                logger.error(f"❌ Failed to fetch geofences. Status Code: {response.status_code}")
                raise httpx.HTTPError(f"❌ Failed to fetch geofences. Status Code: {response.status_code}")

    @classmethod
    async def cache_koji_geofences(cls):
        """Fetch and cache Koji Geofences in Redis."""
        logger.debug(f"🔃 Attempting to cache Koji geofences...")
        redis_status = await cls.redis_manager.check_redis_connection()
        if not redis_status:
            logger.error("❌ Redis is not connected. Cannot retrieve cached geofences.")
            return

        geofences = await cls.get_koji_geofences()
        if geofences:
            await cls.redis_manager.redis_client.set(cls.geofence_key, json.dumps(geofences), cls.expiry)
            logger.success(f"✅ Cached {len(geofences)} Koji Geofences for {cls.expiry} seconds.")
        else:
            logger.warning("❌ No geofences retrieved. Cache was not udpated")
            return

    @classmethod
    async def get_cached_geofences(cls):
        """Retrieve cached Koji Geofences from Redis"""
        redis_status = await cls.redis_manager.check_redis_connection()
        if not redis_status:
            logger.error("❌ Redis is not connected. Cannot retrieve cached geofences.")
            return None

        cached_geofences = await cls.redis_manager.redis_client.get(cls.geofence_key)
        if cached_geofences:
            result = json.loads(cached_geofences)
            logger.debug(f"✅ Retrieved cached geofences: {result}")
            return result
        elif cached_geofences is None:
            logger.warning("⚠️ No cached geofences found.")
            re_cache_koji_geofences = await cls.cache_koji_geofences()
            if re_cache_koji_geofences:
                cached_geofences = await cls.redis_manager.redis_client.get(cls.geofence_key)
                result = json.loads(cached_geofences)
                logger.debug(f"✅ Retrieved cached geofences: {result}")
                return result
        else:
            logger.error("Failed to retrieve cached geofences.")
            return

    async def refresh_geofences(self):
        """Continuously refresh Koji Geofences every `self.refresh_interval` seconds."""
        redis_status = await self.redis_manager.check_redis_connection()
        if not redis_status:
            logger.error("❌ Redis is not connected. Cannot retrieve cached geofences.")
            return

        while True:
            logger.info("🔃 Refreshing Koji Geofences...")
            await self.cache_koji_geofences()
            # After caching, retrieve the updated geofences
            new_geofences = await self.get_cached_geofences()
            if new_geofences:
                global_state.geofences = new_geofences
                logger.info(f"✅ Updated Koji Geofences: {len(new_geofences)} cached geofences")
            # ✅ Calculate next refresh timestamp
            next_refresh_time = datetime.now() + timedelta(seconds=self.refresh_interval)
            next_refresh_str = next_refresh_time.strftime("%Y-%m-%d %H:%M:%S")

            logger.info(f"⏰ Next geofence refresh scheduled at: {next_refresh_str}")
            await asyncio.sleep(self.refresh_interval)
