import requests
import json
import httpx
import config as AppConfig
from utils.logger import logger
from my_redis.connect_redis import redis_client

async def get_koji_geofences():
    """Get Koji Geofences from the API."""
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {AppConfig.koji_bearer_token}"}
        response = await client.get(AppConfig.koji_geofence_api_url, headers=headers)
        if response.status_code == 200:
            return response.json().get("data", {}).get("features", [])
        else:
            # This needs to be improved to handle multi polygon error.
            logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
            raise httpx.HTTPError(f"Failed to fetch geofences. Status Code: {response.status_code}")

async def cache_koji_geofences():
    """Cache Koji Geofences in Redis."""
    geofences = await get_koji_geofences()
    if not redis_client:
        logger.error("Redis is not connected.")
        return
    # Add Koji geofences to Redis for an hour
    # Once an hour goes by we ask for get_koji_geofences again to spot if there are new geofences
