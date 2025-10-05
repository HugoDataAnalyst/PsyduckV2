from __future__ import annotations
import asyncio
import json
import httpx
import redis
from shapely import area
import config as AppConfig
from datetime import datetime, timedelta
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from sql.connect_db import execute, fetch_one
from dataclasses import dataclass
from server_fastapi import global_state
from timezonefinder import TimezoneFinder
from shapely.geometry import Polygon, shape
import pytz

@dataclass
class _AreaObj:
    id: int
    name: str

class KojiGeofences:
    """
    Koji Geofences manager (Polygon-only):
      - fetch from Koji API
      - ensure areas exist in DB
      - cache in Redis (only when changed)
      - in-memory refresh loop
    """
    geofence_key = "koji_geofences"
    expiry = AppConfig.geofence_expire_cache_seconds
    bearer_token = AppConfig.koji_bearer_token
    geofence_api_url = AppConfig.koji_geofence_api_url
    redis_manager = RedisManager()  # Singleton
    tf = TimezoneFinder()
    _instance = None  # Singleton instance

    def __new__(cls, refresh_interval: int):
        """Ensures only one KojiGeofences instance is created globally (Singleton)."""
        if cls._instance is None:
            cls._instance = super(KojiGeofences, cls).__new__(cls)
            cls._instance.refresh_interval = int(refresh_interval)
        return cls._instance

    # DB helpers

    @staticmethod
    async def _get_or_create_area(area_name: str) -> tuple[_AreaObj, bool]:
        """
        Ensure an area exists and return (area_obj, created).
        """
        name = " ".join((area_name or "").split())

        # 1) Try read
        row = await fetch_one("SELECT id FROM area_names WHERE name = %s", (name,))
        if row and "id" in row:
            return _AreaObj(id=int(row["id"]), name=name), False

        # 2) Insert if missing (race-safe)
        ins = await execute("INSERT IGNORE INTO area_names (name) VALUES (%s)", (name,))
        created = getattr(ins, "rowcount", 0) == 1

        # 3) Fetch id
        row2 = await fetch_one("SELECT id FROM area_names WHERE name = %s", (name,))
        if not row2 or "id" not in row2:
            raise RuntimeError(f"Failed to ensure area '{name}' in area_names.")
        return _AreaObj(id=int(row2["id"]), name=name), created

    # Redis helpers

    @classmethod
    async def get_redis_client(cls):
        """
        Retrieve a validated Redis connection with smart reconnection handling.
        """
        if not await cls.redis_manager.check_redis_connection():
            logger.error("❌ Redis connection unavailable after retries")
            return None

        # Return the active client if healthy
        if cls.redis_manager.redis_client:
            logger.debug("✅ Using existing Redis connection")
            return cls.redis_manager.redis_client

        logger.error("❌ No Redis client instance available")
        return None

    @classmethod
    async def _read_cached(cls):
        client = await cls.get_redis_client()
        if not client:
            return None
        raw = await client.get(cls.geofence_key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception as e:
            logger.warning(f"⚠️ Cached geofences JSON parse failed: {e}")
            return None

    @classmethod
    def _diff(cls, old: list | None, new: list) -> tuple[list[str], list[str]]:
        old_names = {g.get("name", "") for g in (old or [])}
        new_names = {g.get("name", "") for g in new}
        added = sorted(n for n in (new_names - old_names) if n)
        removed = sorted(n for n in (old_names - new_names) if n)
        return added, removed

    # Koji fetch & parse Polygon-only

    @classmethod
    async def get_koji_geofences(cls):
        """
        Fetch Koji Geofences from the API and extract relevant details.

        Rules:
          - Accept ONLY 'Polygon' geometry.
          - Skip all other types (e.g., MultiPolygon) with a warning.
          - Store Polygon coordinates as a list of rings.
          - Timezone offset computed from geometry centroid.
        """
        headers = {"Authorization": f"Bearer {cls.bearer_token}"} if cls.bearer_token else {}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(cls.geofence_api_url, headers=headers)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.error(f"❌ Failed to fetch geofences: {e} (status={resp.status_code})")
                raise

            try:
                payload = resp.json()
            except Exception as e:
                logger.error(f"❌ Invalid JSON from Koji API: {e}")
                raise

        features = (payload.get("data") or {}).get("features") or []
        geofences = []

        for f in features:
            props = f.get("properties") or {}
            geom = f.get("geometry") or {}
            gtype = geom.get("type")
            name = (props.get("name") or "Unknown").strip()

            if gtype != "Polygon":
                # explicitly skip everything that isn't Polygon
                logger.warning(f"⛔ Skipping non-Polygon geofence '{name}' (type={gtype})")
                continue

            # Build shapely geometry for centroid/tz
            shp = None
            try:
                shp = shape(geom)
            except Exception as e:
                logger.warning(f"⚠️ Invalid Polygon geometry for '{name}': {e}")
                continue

            # Ensure area exists in DB
            area_obj, created = await cls._get_or_create_area(name)
            if created:
                logger.debug(f"✅ Created new area: {name}")
            else:
                logger.debug(f"↔️ Existing area: '{name}' id={area_obj.id}")

            # Normalize Polygon coordinates into rings
            coords_to_store = []
            rings = (geom.get("coordinates") or [])
            for ring in rings:
                coords_to_store.append([list(c) for c in ring])  # ensure lists, not tuples

            # Timezone offset via centroid
            offset = 0
            try:
                c = shp.centroid
                tz_str = cls.tf.timezone_at(lng=c.x, lat=c.y)
                if tz_str:
                    tz = pytz.timezone(tz_str)
                    now = datetime.now(tz)
                    off = now.utcoffset()
                    offset = int(round((off.total_seconds() / 3600) if off else 0))
                else:
                    logger.warning(f"⚠️ Timezone not determined for area '{name}'")
            except Exception as e:
                logger.warning(f"⚠️ Could not calculate offset for '{name}': {e}")

            geofences.append({
                "id": area_obj.id,
                "name": name,
                "offset": offset,       # integer hours
                "coordinates": coords_to_store,
                "geometry_type": "Polygon",
            })

        geofences.sort(key=lambda g: (g["name"], g["id"]))
        logger.debug(f"✅ Parsed {len(geofences)} Koji geofences (Polygon-only).")
        return geofences

    # Cache writer/reader

    @classmethod
    async def cache_koji_geofences(cls) -> bool:
        """
        Fetch and cache Koji Geofences in Redis. Writes only if changed.
        """
        logger.debug("🔃 Fetching Koji geofences to cache...")
        client = await cls.get_redis_client()
        if not client:
            logger.error("❌ Redis is not connected. Cannot cache Koji geofences.")
            return False

        new_geos = await cls.get_koji_geofences()
        if new_geos is None:
            logger.error("❌ Fetch returned None; not caching.")
            return False

        old_geos = await cls._read_cached()
        added, removed = cls._diff(old_geos, new_geos)
        if added or removed:
            logger.info(f"🗺️ Geofence changes: +{len(added)} / -{len(removed)}")
            if added:
                logger.debug(f"➕ Added: {added}")
            if removed:
                logger.debug(f"➖ Removed: {removed}")
        else:
            logger.debug(f"ℹ️ No geofence name changes detected (count={len(new_geos)}).")

        try:
            await client.set(cls.geofence_key, json.dumps(new_geos), ex=cls.expiry)
            logger.success(f"✅ Cached {len(new_geos)} Koji geofences for {cls.expiry}s.")
            return True
        except Exception as e:
            logger.error(f"❌ Redis set failed: {e}")
            return False

    @classmethod
    async def get_cached_geofences(cls):
        """
        Retrieve cached Koji Geofences from Redis; populate from API if empty.
        """
        data = await cls._read_cached()
        if data is None:
            ok = await cls.cache_koji_geofences()
            if not ok:
                return None
            data = await cls._read_cached()
        return data

    # Refresh loop

    async def refresh_geofences(self):
        """
        Continuously refresh Koji Geofences every `self.refresh_interval` seconds.
        Writes only when changes occur (handled by cache_koji_geofences).
        Always syncs in-memory global_state after caching.
        """
        while True:
            try:
                ok = await self.cache_koji_geofences()
                if ok:
                    current = await self.get_cached_geofences()
                    if current is not None:
                        global_state.geofences = current
                        logger.info(f"✅ Updated Koji geofences in memory: {len(current)} entries")
            except Exception as e:
                logger.exception(f"Geofence refresh cycle failed: {e}")

            next_refresh = datetime.now() + timedelta(seconds=self.refresh_interval)
            logger.info(f"⏰ Next geofence refresh at: {next_refresh:%Y-%m-%d %H:%M:%S}")
            await asyncio.sleep(self.refresh_interval)
