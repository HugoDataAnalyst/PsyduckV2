"""
Redis-backed Global State Manager for Multi-Worker Uvicorn Support.

All workers share state via Redis, with local caching for performance.
This replaces direct access to global_state module-level variables.
"""
import json
import time
from datetime import timezone as dt_timezone
from typing import Any

from utils.logger import logger


class GlobalStateManager:
    """
    Redis-backed shared state with local caching.

    Keys:
      - koji_geofences (existing) - geofence data from Koji
      - cached_pokestops (existing) - pokestop counts per area
      - psyduckv2:state:user_timezone - local timezone string

    Local cache has a 60-second TTL to reduce Redis round-trips during high load.
    Geofences/pokestops rarely change, so longer TTL is safe.
    Falls back to global_state when Redis is unavailable (for resilience).
    """

    # Redis keys
    GEOFENCES_KEY = "koji_geofences"  # Reuse existing key
    POKESTOPS_KEY = "cached_pokestops"  # Reuse existing key
    TIMEZONE_KEY = "psyduckv2:state:user_timezone"

    # Local cache configuration
    # Geofences/pokestops rarely change, so use longer TTL to reduce Redis load
    _local_cache: dict[str, dict[str, Any]] = {}
    _local_cache_ttl = 3600.0  # seconds - longer TTL reduces Redis pressure during high load

    # Redis manager will be set at startup
    _redis_manager = None

    @classmethod
    def set_redis_manager(cls, redis_manager):
        """Set the Redis manager instance. Must be called at startup."""
        cls._redis_manager = redis_manager

    @classmethod
    def _is_cache_valid(cls, key: str) -> bool:
        """Check if local cache entry is still valid."""
        if key not in cls._local_cache:
            return False
        entry = cls._local_cache[key]
        return (time.monotonic() - entry["timestamp"]) < cls._local_cache_ttl

    @classmethod
    def _update_local_cache(cls, key: str, data: Any):
        """Update local cache entry."""
        cls._local_cache[key] = {
            "data": data,
            "timestamp": time.monotonic()
        }

    @classmethod
    def _get_from_local_cache(cls, key: str) -> Any | None:
        """Get data from local cache if valid."""
        if cls._is_cache_valid(key):
            return cls._local_cache[key]["data"]
        return None

    @classmethod
    def invalidate_local_cache(cls, key: str | None = None):
        """Invalidate local cache for a specific key or all keys."""
        if key:
            cls._local_cache.pop(key, None)
        else:
            cls._local_cache.clear()

    # Geofences

    @classmethod
    async def get_geofences(cls) -> list | None:
        """
        Get geofences from local cache or Redis.

        Returns list of geofence dicts or None if not available.
        Falls back to global_state if Redis is unavailable (for resilience during high load).
        """
        # Check local cache first (fast path - no Redis call)
        cached = cls._get_from_local_cache(cls.GEOFENCES_KEY)
        if cached is not None:
            return cached

        # Fetch from Redis
        try:
            if not cls._redis_manager:
                logger.warning("GlobalStateManager: Redis manager not set")
                return cls._fallback_to_global_state_geofences()

            client = await cls._redis_manager.check_redis_connection()
            if not client:
                logger.warning("GlobalStateManager: Redis connection unavailable")
                return cls._fallback_to_global_state_geofences()

            raw = await client.get(cls.GEOFENCES_KEY)
            if raw:
                data = json.loads(raw)
                cls._update_local_cache(cls.GEOFENCES_KEY, data)
                # Also update global_state so fallback stays fresh
                cls._sync_geofences_to_global_state(data)
                return data

            return cls._fallback_to_global_state_geofences()

        except Exception as e:
            logger.warning(f"GlobalStateManager: Redis error getting geofences: {e}, using fallback")
            return cls._fallback_to_global_state_geofences()

    @classmethod
    def _sync_geofences_to_global_state(cls, geofences: list) -> None:
        """
        Keep global_state.geofences in sync with Redis data.
        This ensures the fallback always has fresh data.
        """
        from server_fastapi import global_state
        global_state.geofences = geofences

    @classmethod
    def _fallback_to_global_state_geofences(cls) -> list | None:
        """
        Fallback to global_state.geofences when Redis is unavailable.
        This ensures webhook processing continues during Redis connection issues.
        """
        from server_fastapi import global_state
        if global_state.geofences:
            # Update local cache so we don't keep hitting this fallback
            cls._update_local_cache(cls.GEOFENCES_KEY, global_state.geofences)
            return global_state.geofences
        return None

    @classmethod
    async def set_geofences(cls, geofences: list, expiry: int | None = None):
        """
        Set geofences in Redis and local cache.

        Args:
            geofences: List of geofence dicts
            expiry: Optional TTL in seconds (uses existing key behavior if None)
        """
        try:
            if not cls._redis_manager:
                logger.warning("GlobalStateManager: Redis manager not set")
                return False

            client = await cls._redis_manager.check_redis_connection()
            if not client:
                logger.warning("GlobalStateManager: Redis connection unavailable")
                return False

            # Store in Redis (with optional expiry)
            if expiry:
                await client.set(cls.GEOFENCES_KEY, json.dumps(geofences), ex=expiry)
            else:
                await client.set(cls.GEOFENCES_KEY, json.dumps(geofences))

            # Update local cache
            cls._update_local_cache(cls.GEOFENCES_KEY, geofences)
            logger.debug(f"GlobalStateManager: Set {len(geofences)} geofences")
            return True

        except Exception as e:
            logger.error(f"GlobalStateManager: Error setting geofences: {e}")
            return False

    # Pokestops

    @classmethod
    async def get_pokestops(cls) -> dict | None:
        """
        Get cached pokestops from local cache or Redis.

        Returns dict with "areas" and "grand_total" or None.
        """
        # Check local cache first
        cached = cls._get_from_local_cache(cls.POKESTOPS_KEY)
        if cached is not None:
            return cached

        # Fetch from Redis
        try:
            if not cls._redis_manager:
                return None

            client = await cls._redis_manager.check_redis_connection()
            if not client:
                return None

            raw = await client.get(cls.POKESTOPS_KEY)
            if raw:
                data = json.loads(raw)
                cls._update_local_cache(cls.POKESTOPS_KEY, data)
                return data

            return None

        except Exception as e:
            logger.error(f"GlobalStateManager: Error getting pokestops: {e}")
            return None

    @classmethod
    async def set_pokestops(cls, pokestops: dict, expiry: int | None = None):
        """Set cached pokestops in Redis and local cache."""
        try:
            if not cls._redis_manager:
                return False

            client = await cls._redis_manager.check_redis_connection()
            if not client:
                return False

            if expiry:
                await client.set(cls.POKESTOPS_KEY, json.dumps(pokestops), ex=expiry)
            else:
                await client.set(cls.POKESTOPS_KEY, json.dumps(pokestops))

            cls._update_local_cache(cls.POKESTOPS_KEY, pokestops)
            return True

        except Exception as e:
            logger.error(f"GlobalStateManager: Error setting pokestops: {e}")
            return False

    # Timezone

    @classmethod
    async def get_timezone(cls):
        """
        Get user timezone from local cache or Redis.

        Returns timezone object or None.
        """
        from zoneinfo import ZoneInfo

        # Check local cache first
        cached = cls._get_from_local_cache(cls.TIMEZONE_KEY)
        if cached is not None:
            return cached

        # Fetch from Redis
        try:
            if not cls._redis_manager:
                return None

            client = await cls._redis_manager.check_redis_connection()
            if not client:
                return None

            raw = await client.get(cls.TIMEZONE_KEY)
            if raw:
                # Convert timezone string to ZoneInfo object
                try:
                    tz = ZoneInfo(raw)
                except Exception:
                    # Fallback for pytz-style timezones
                    import pytz
                    tz = pytz.timezone(raw)

                cls._update_local_cache(cls.TIMEZONE_KEY, tz)
                return tz

            return None

        except Exception as e:
            logger.error(f"GlobalStateManager: Error getting timezone: {e}")
            return None

    @classmethod
    async def set_timezone(cls, timezone_obj):
        """
        Set user timezone in Redis and local cache.

        Args:
            timezone_obj: A timezone object (pytz, ZoneInfo, etc.)
        """
        try:
            if not cls._redis_manager:
                return False

            client = await cls._redis_manager.check_redis_connection()
            if not client:
                return False

            # Store timezone as string (e.g., "Europe/Lisbon")
            tz_str = str(timezone_obj)
            await client.set(cls.TIMEZONE_KEY, tz_str)

            cls._update_local_cache(cls.TIMEZONE_KEY, timezone_obj)
            logger.debug(f"GlobalStateManager: Set timezone to {tz_str}")
            return True

        except Exception as e:
            logger.error(f"GlobalStateManager: Error setting timezone: {e}")
            return False

    # Sync helpers
    @classmethod
    async def wait_for_state(cls, timeout: float = 30.0) -> bool:
        """
        Wait for essential state to be populated by the leader.

        Returns True if state is available, False if timeout reached.
        """
        import asyncio

        start = time.monotonic()
        while (time.monotonic() - start) < timeout:
            geofences = await cls.get_geofences()
            if geofences is not None:
                logger.info("GlobalStateManager: State available from leader")
                return True

            await asyncio.sleep(0.5)

        logger.warning(f"GlobalStateManager: Timeout waiting for state ({timeout}s)")
        return False

    @classmethod
    async def sync_to_legacy_global_state(cls):
        """
        Sync Redis state to legacy global_state module for backward compatibility.

        This allows existing code that reads global_state directly to continue working.
        """
        from server_fastapi import global_state

        geofences = await cls.get_geofences()
        if geofences is not None:
            global_state.geofences = geofences

        pokestops = await cls.get_pokestops()
        if pokestops is not None:
            global_state.cached_pokestops = pokestops

        timezone = await cls.get_timezone()
        if timezone is not None:
            global_state.user_timezone = timezone

        logger.debug("GlobalStateManager: Synced to legacy global_state")
