import asyncio
import json
import httpx
import config as AppConfig
from datetime import datetime, timedelta
from utils.logger import logger
from my_redis.connect_redis import redis_client

class KojiGeofences:
    """Koji Geofences class.
    Handles fetching, caching and retrieving Koji Geofences from its API.
    """

    # Start the class static variables
    geofence_key = "koji_geofences"
    expiry = 3600
    bearer_token = AppConfig.koji_bearer_token
    geofence_api_url = AppConfig.koji_geofence_api_url

    def __init__(self, refresh_interval):
        """Instance-level refresh interval"""
        self.refresh_interval = refresh_interval


    @classmethod
    async def get_koji_geofences(cls):
        """Get Koji Geofences from the API."""
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {cls.bearer_token}"}
            response = await client.get(cls.geofence_api_url, headers=headers)
            if response.status_code == 200:
                return response.json().get("data", {}).get("features", [])
            else:
                # This needs to be improved to handle multi polygon error.
                logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
                raise httpx.HTTPError(f"Failed to fetch geofences. Status Code: {response.status_code}")

    @classmethod
    async def cache_koji_geofences(cls):
        """Fetch and cache Koji Geofences in Redis."""
        geofences = await cls.get_koji_geofences()
        if not redis_client:
            logger.error("Redis is not connected.")
            return
        if geofences:
            await redis_client.set(cls.geofence_key, json.dumps(geofences), cls.expiry)
            logger.success(f"Cached {len(geofences)} Koji Geofences for {cls.expiry} seconds.")
        else:
            logger.warning("No geofences retrieved. Cache was not udpated")
            return

    @classmethod
    async def get_cached_geofences(cls):
        """Retrieve cached Koji Geofences from Redis"""
        if not redis_client:
            logger.error("Redis is not connected.")
            return
        cached_geofences = await redis_client.get(cls.geofence_key)
        if cached_geofences:
            result = json.loads(cached_geofences)
            logger.debug(f"Retrieved cached geofences: {result}")
            return result
        elif cached_geofences is None:
            logger.warning("No cached geofences found.")
            re_cache_koji_geofences = await cls.cache_koji_geofences()
            if re_cache_koji_geofences:
                cached_geofences = await redis_client.get(cls.geofence_key)
                result = json.loads(cached_geofences)
                logger.debug(f"Retrieved cached geofences: {result}")
                return result
        else:
            logger.error("Failed to retrieve cached geofences.")
            return

    async def refresh_geofences(self):
        """Continuously refresh Koji Geofences every `self.refresh_interval` seconds."""
        while True:
            logger.info("Refreshing Koji Geofences...")
            await self.cache_koji_geofences()
            # ✅ Calculate next refresh timestamp
            next_refresh_time = datetime.now() + timedelta(seconds=self.refresh_interval)
            next_refresh_str = next_refresh_time.strftime("%Y-%m-%d %H:%M:%S")

            logger.info(f"Next geofence refresh scheduled at: {next_refresh_str}")
            await asyncio.sleep(self.refresh_interval)
