from redis.asyncio.client import Redis
from utils.safe_values import _safe_int, _to_float
from utils.logger import logger
from datetime import datetime
from sql.tasks.pokemon_processor import PokemonSQLProcessor
from sql.utils.errors import FlushError
from my_redis.queries.buffer import flush_guard
import config as AppConfig
import asyncio


async def _get_client_with_retry(redis_client: Redis | None, max_attempts: int = 3, delay: float = 0.3) -> Redis | None:
    """
    Try to get a working Redis client with retry logic.
    First tries the provided client, then falls back to getting a fresh connection.
    """
    from my_redis.connect_redis import RedisManager
    redis_manager = RedisManager()

    for attempt in range(1, max_attempts + 1):
        # Try provided client first
        if redis_client:
            try:
                if await redis_client.ping():
                    return redis_client
            except Exception:
                pass

        # Fall back to getting a fresh connection
        try:
            client = await redis_manager.get_connection_with_retry(max_attempts=2, delay=0.2)
            if client:
                return client
        except Exception:
            pass

        if attempt < max_attempts:
            await asyncio.sleep(delay)
            logger.debug(f"⏳ Buffer retry attempt {attempt}/{max_attempts}")

    return None

class PokemonIVRedisBuffer:
    redis_key = "buffer:pokemon_iv_events"
    redis_coords_key = "buffer:pokemon_iv_coords"
    aggregation_threshold = int(AppConfig.pokemon_max_threshold)

    @classmethod
    async def increment_event(cls, redis_client: Redis, event_data: dict):
        try:
            # Construct a unique key based on event fields
            spawnpoint = event_data.get("spawnpoint")
            pokemon_id = int(event_data.get("pokemon_id"))
            form       = str(event_data.get("form", 0))
            round_iv   = int(round(event_data.get("iv")))
            level      = int(event_data.get("level"))
            area_id    = int(event_data.get("area_id"))
            first_seen = int(event_data.get("first_seen"))
            latitude   = _to_float(event_data.get("latitude"))
            longitude  = _to_float(event_data.get("longitude"))

            if None in [spawnpoint, pokemon_id, round_iv, area_id, first_seen]:
                logger.warning("❌ Event missing required fields. Skipping.")
                return

            # Create a composite unique key string
            unique_key = f"{spawnpoint}|{pokemon_id}|{form}|{round_iv}|{level}|{area_id}|{first_seen}"

            # Get a working client with retry
            client = await _get_client_with_retry(redis_client)
            if not client:
                logger.error("❌ Pokemon IV buffer: No Redis connection available after retries. Event lost.")
                return

            # Persist coordinates once per spawnpoint
            if latitude is not None and longitude is not None:
                try:
                    await client.hsetnx(cls.redis_coords_key, spawnpoint, f"{latitude},{longitude}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to cache coords for spawnpoint {spawnpoint}: {e}")

            # Append one event line with retry
            new_len = None
            for attempt in range(3):
                try:
                    new_len = await client.rpush(cls.redis_key, unique_key)
                    break
                except Exception as e:
                    if attempt < 2:
                        client = await _get_client_with_retry(None)
                        if not client:
                            logger.error(f"❌ Pokemon IV buffer: rpush failed, no client available: {e}")
                            return
                    else:
                        logger.error(f"❌ Pokemon IV buffer: rpush failed after retries: {e}")
                        return

            logger.debug(f"Appended IV event '{unique_key}'. List length now {new_len}.")

            # Check list size
            current_len = await client.llen(cls.redis_key)
            logger.debug(f"📊 👻 Current queued pokemon events: {current_len}")

            # Flush if the number of unique lines exceeds the threshold
            if current_len >= cls.aggregation_threshold:
                logger.warning(f"📊 👻 Event buffer threshold reached: {current_len}. Flushing…")
                await cls.flush_if_ready(client)
        except Exception as e:
            logger.error(f"❌ Error incrementing aggregated event: {e}")

    @staticmethod
    def _parse_iv_lines(lines, coords_raw) -> tuple[list[dict], dict]:
        """Turn buffered lines (+ the coords snapshot) into SQL rows."""
        coords_map = {}
        for k, v in (coords_raw or {}).items():
            try:
                sp = k.decode() if isinstance(k, (bytes, bytearray)) else k
                s_ = v.decode() if isinstance(v, (bytes, bytearray)) else v
                la, lo = s_.split(",", 1)
                coords_map[sp] = (float(la), float(lo))
            except Exception:
                pass

        data_batch = []
        stats = {"lines": len(lines), "malformed": 0, "coords_used": 0, "coords_missing": 0}
        for b in lines:
            try:
                line = b.decode() if isinstance(b, (bytes, bytearray)) else b
                # spawnpoint|pokemon_id|form|iv|level|area_id|first_seen
                sp_hex, pid_s, form_s, iv_s, level_s, area_s, fs_s = line.split("|")
                lat, lon = coords_map.get(sp_hex, (None, None))
                if lat is None or lon is None:
                    stats["coords_missing"] += 1
                else:
                    stats["coords_used"] += 1
                data_batch.append({
                    "spawnpoint": sp_hex,
                    "pokemon_id": int(pid_s),
                    "form": form_s,
                    "iv": int(iv_s),
                    "level": int(level_s),
                    "area_id": int(area_s),
                    "first_seen": int(fs_s),  # epoch secs (local area time)
                    "latitude": lat,
                    "longitude": lon,
                })
            except Exception:
                stats["malformed"] += 1
        return data_batch, stats

    @classmethod
    async def _flush(cls, redis_client: Redis, tag: str, label: str) -> int:
        """
        Move the event list and its coords hash aside, write them to MySQL, and
        delete them only on success. A failed write merges both back into the
        live buffers so the next flush retries them (INSERT IGNORE makes a
        retried batch safe). Returns the number of rows inserted.
        """
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error(f"❌ {label}: No Redis connection available after retries.")
            return 0

        main_temp = await flush_guard.claim(client, cls.redis_key, tag)
        if main_temp is None:
            logger.debug(f"📭 No {label} data to flush.")
            return 0

        coords_temp = None
        try:
            coords_temp = await flush_guard.claim(client, cls.redis_coords_key, tag)
            lines = await client.lrange(main_temp, 0, -1)
            coords_raw = await client.hgetall(coords_temp) if coords_temp else {}
            data_batch, st = cls._parse_iv_lines(lines, coords_raw)
            logger.info(
                f"📤 {label}: lines={st['lines']}, batch={len(data_batch)}, "
                f"coords_used={st['coords_used']}, coords_missing={st['coords_missing']}, malformed={st['malformed']}"
            )
            if not data_batch:
                await flush_guard.discard(client, main_temp, coords_temp)
                return 0
            inserted_count = await PokemonSQLProcessor.bulk_insert_iv_daily_events(data_batch)
        except Exception as e:
            logger.error(f"❌ {label} failed, keeping the batch for retry: {e}")
            await flush_guard.requeue(client, main_temp, cls.redis_key)
            await flush_guard.requeue(client, coords_temp, cls.redis_coords_key)
            return 0

        await flush_guard.discard(client, main_temp, coords_temp)
        logger.success(f"📬 {label} inserted +{inserted_count} rows into MySQL.")
        return inserted_count

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        """Flush buffered IV events into MySQL. Returns rows inserted."""
        return await cls._flush(redis_client, "flushing", "IV daily-events")

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        """Force flush all buffered IV events, regardless of threshold."""
        return await cls._flush(redis_client, "force_flushing", "FORCE IV daily-events")


class ShinyRateRedisBuffer:
    redis_key = "buffer:agg_shiny_rates_hash"
    aggregation_threshold = int(AppConfig.shiny_max_threshold)

    @classmethod
    async def increment_event(cls, redis_client: Redis, event_data: dict):
        try:
            # Required fields for constructing the unique key
            username = event_data.get("username")
            pokemon_id = event_data.get("pokemon_id")
            form = event_data.get("form", 0)
            shiny_raw = event_data.get("shiny", 0)
            if isinstance(shiny_raw, bool):
                shiny = 1 if shiny_raw else 0
            else:
                shiny_num = _safe_int(shiny_raw, 0)
                shiny = 1 if shiny_num and shiny_num != 0 else 0
            area_id = event_data.get("area_id")
            first_seen = event_data.get("first_seen")
            # Golbat sends username = null when it re-saves a pokemon without a
            # reporting account - e.g. its weather-change IV recalculation
            # (ProactiveIVSwitch) with store_username off - so expect ['username'].
            required = {"username": username, "pokemon_id": pokemon_id, "area_id": area_id, "first_seen": first_seen}
            missing = [name for name, value in required.items() if value is None]
            if missing:
                logger.warning(
                    f"❌ Shiny event missing required fields {missing} "
                    f"(pokemon_id={pokemon_id}, encounter_id={event_data.get('encounter_id')}). Skipping."
                )
                return

            dt = datetime.fromtimestamp(first_seen)
            month_year = dt.strftime("%y%m")  # e.g. "2503" for March 2025

            # Construct a composite unique key
            unique_key = f"{username}|{pokemon_id}|{form}|{shiny}|{area_id}|{month_year}"

            # Get a working client with retry
            client = await _get_client_with_retry(redis_client)
            if not client:
                logger.error("❌ Shiny buffer: No Redis connection available after retries. Event lost.")
                return

            # Increment the count for this unique key with retry
            new_count = None
            for attempt in range(3):
                try:
                    new_count = await client.hincrby(cls.redis_key, unique_key, 1)
                    break
                except Exception as e:
                    if attempt < 2:
                        client = await _get_client_with_retry(None)
                        if not client:
                            logger.error(f"❌ Shiny buffer: hincrby failed, no client available: {e}")
                            return
                    else:
                        logger.error(f"❌ Shiny buffer: hincrby failed after retries: {e}")
                        return

            logger.debug(f"Incremented shiny key '{unique_key}' to {new_count}.")

            # Check total unique keys in the hash
            current_unique_count = await client.hlen(cls.redis_key)
            logger.debug(f"📊 🌟 Current total unique aggregated shiny keys: {current_unique_count}")

            # Flush if threshold is reached
            if current_unique_count >= cls.aggregation_threshold:
                logger.warning(f"📊 🌟 Shiny aggregation threshold reached: {current_unique_count} unique keys. Initiating flush...")
                await cls.flush_if_ready(client)
        except Exception as e:
            logger.error(f"❌ Error incrementing aggregated shiny event: {e}")

    @staticmethod
    def _parse_shiny(aggregated_data) -> tuple[list[dict], int]:
        data_batch = []
        malformed = 0
        for k, v in aggregated_data.items():
            try:
                composite_key = k.decode("utf-8") if isinstance(k, bytes) else k
                # username|pokemon_id|form|shiny|area_id|YYMM
                parts = composite_key.split("|")
                if len(parts) != 6:
                    malformed += 1
                    continue
                username, pid_s, form_s, shiny_s, area_s, yymm_s = parts
                data_batch.append({
                    "username": username,
                    "pokemon_id": int(pid_s),
                    "form": form_s,
                    "shiny": int(shiny_s),
                    "area_id": int(area_s),
                    "first_seen": int(datetime.strptime(yymm_s, "%y%m").timestamp()),
                    "increment": int(v),
                })
            except Exception:
                malformed += 1
        return data_batch, malformed

    @classmethod
    async def _flush(cls, redis_client: Redis, tag: str, label: str) -> int:
        """
        Move the counts aside, upsert them, and delete them only on success.

        total_count is additive, so a retry must never resend rows that were
        already committed: the upsert commits chunk by chunk and reports the
        unwritten rows, and only those are added back to the live buffer.
        """
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error(f"❌ {label}: No Redis connection available after retries.")
            return 0

        temp_key = await flush_guard.claim(client, cls.redis_key, tag)
        if temp_key is None:
            logger.debug(f"📭 No {label} data to flush.")
            return 0

        try:
            aggregated_data = await client.hgetall(temp_key)
            data_batch, malformed = cls._parse_shiny(aggregated_data)
            logger.info(f"📤 {label}: keys={len(aggregated_data)}, batch={len(data_batch)}, malformed={malformed}")
            if not data_batch:
                await flush_guard.discard(client, temp_key)
                return 0
            inserted_count = await PokemonSQLProcessor.bulk_upsert_shiny_username_rate_batch(data_batch)
        except FlushError as e:
            if e.remaining is None:
                logger.error(f"❌ {label} failed, keeping the batch for retry: {e}")
                await flush_guard.requeue(client, temp_key, cls.redis_key)
                return 0
            counts = {
                f"{u}|{pid}|{form}|{shiny}|{area}|{my:04d}": inc
                for (u, pid, form, shiny, area, my, inc) in e.remaining
            }
            logger.error(f"❌ {label} partly written; re-queuing {len(counts)} unwritten entries: {e}")
            try:
                await flush_guard.requeue_counts(client, cls.redis_key, counts)
                await flush_guard.discard(client, temp_key)
            except Exception as re_err:
                # Keep the temp key: a later orphan recovery may re-add the
                # committed part too, but losing the unwritten counts is worse.
                logger.error(f"❌ {label}: could not re-queue unwritten entries ({re_err}); batch left in {temp_key}")
            return 0
        except Exception as e:
            logger.error(f"❌ {label} failed, keeping the batch for retry: {e}")
            await flush_guard.requeue(client, temp_key, cls.redis_key)
            return 0

        await flush_guard.discard(client, temp_key)
        logger.success(f"✨ {label} upserted +{inserted_count} rows into MySQL.")
        return inserted_count

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        """Flush buffered shiny aggregates into MySQL. Returns rows upserted."""
        return await cls._flush(redis_client, "flushing", "Shiny rates")

    @classmethod
    async def force_flush(cls, redis_client: Redis):
        """Force flush all buffered shiny data regardless of thresholds."""
        return await cls._flush(redis_client, "force_flushing", "FORCE shiny rates")
