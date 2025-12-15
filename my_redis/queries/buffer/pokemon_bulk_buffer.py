from redis.asyncio.client import Redis
from utils.safe_values import _safe_int, _to_float
from utils.logger import logger
from datetime import datetime
from sql.tasks.pokemon_processor import PokemonSQLProcessor
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

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        """
        Flush buffered IV aggregates into MySQL.
        - Renames both the aggregate hash and its companion coords hash.
        - Builds a data_batch (skipping malformed keys).
        - Uses the v2 temp-table path for fast upsert.
        - Returns the number of aggregated rows consumed by SQL (not unique keys in Redis).
        """
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Pokemon IV flush: No Redis connection available after retries.")
            return 0

        main_temp = None
        coords_temp = None
        try:
            # 0) Quick check
            if not await client.exists(cls.redis_key):
                logger.debug("📭 No aggregated IV data to flush.")
                return 0

            # 1) Atomically rename the main aggregate hash and the companion coords hash
            main_temp = cls.redis_key + ":flushing"            # e.g., buffer:pokemon_iv_events:flushing
            coords_temp = cls.redis_coords_key + ":flushing"   # e.g., buffer:pokemon_iv_coords:flushing

            try:
                await client.rename(cls.redis_key, main_temp)
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("📭 IV list disappeared before rename. Nothing to flush.")
                    return 0
                raise

            # coords are optional
            try:
                await client.rename(cls.redis_coords_key, coords_temp)
            except Exception as rename_coords_err:
                if "no such key" in str(rename_coords_err).lower():
                    coords_temp = None  # proceed without coords
                else:
                    raise

            # 2) Read both hashes
            lines = await client.lrange(main_temp, 0, -1)
            if not lines:
                logger.debug("📭 Temp IV list empty after rename. Skipping")
                return 0

            total_lines = len(lines)

            # snapshot coords (from temp hash if present)
            coords_map = {}
            if coords_temp:
                coords_raw = await client.hgetall(coords_temp)
                for k, v in coords_raw.items():
                    try:
                        sp = k.decode() if isinstance(k, (bytes, bytearray)) else k
                        s  = v.decode() if isinstance(v, (bytes, bytearray)) else v
                        la, lo = s.split(",", 1)
                        coords_map[sp] = (float(la), float(lo))
                    except Exception:
                        pass

            # 3) Build the SQL batch
            data_batch = []
            malformed = 0
            used_coords = 0
            missing_coords = 0

            for b in lines:
                try:
                    line = b.decode() if isinstance(b, (bytes, bytearray)) else b
                    # spawnpoint|pokemon_id|form|iv|level|area_id|first_seen
                    sp_hex, pid_s, form_s, iv_s, level_s, area_s, fs_s = line.split("|")
                    lat, lon = coords_map.get(sp_hex, (None, None))
                    if lat is None or lon is None:
                        missing_coords += 1
                    else:
                        used_coords += 1

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
                    malformed += 1

            logger.info(
                f"📤 Flushing IV events: lines={total_lines}, batch={len(data_batch)}, "
                f"coords_used={used_coords}, coords_missing={missing_coords}, malformed={malformed}"
            )

            if not data_batch:
                return 0

            # 4) Upsert into SQL
            inserted_count = await PokemonSQLProcessor.bulk_insert_iv_daily_events(data_batch)
            logger.success(f"📬 IV daily-events inserted +{inserted_count} rows into MySQL.")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Error during IV events flush: {e}", exc_info=True)
            return 0
        finally:
            # 5) Cleanup temp keys
            try:
                if main_temp and client:
                    await client.delete(main_temp)
            except Exception:
                pass
            try:
                if coords_temp and client:
                    await client.delete(coords_temp)
            except Exception:
                pass


    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        """
        Force flush all buffered IV aggregates, regardless of threshold.
        Mirrors flush_if_ready logic:
          - rename BOTH main and coords hashes with ':force_flushing'
          - read + parse 6-part composite key (sp|pid|form|bucket|area|yymm)
          - join coords if available
          - upsert via v2 SQL path
        Returns number of rows consumed by SQL.
        """
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Pokemon IV force-flush: No Redis connection available after retries.")
            return 0

        main_temp = None
        coords_temp = None
        try:
            # quick existence check
            if not await client.exists(cls.redis_key):
                logger.debug("📭 No Pokémon IV data to force flush")
                return 0

            # rename the list to a force temp
            main_temp = cls.redis_key + ":force_flushing"
            try:
                await client.rename(cls.redis_key, main_temp)
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("📭 IV hash disappeared before force rename. Nothing to flush.")
                    return 0
                raise

            # rename coords if present
            coords_temp = cls.redis_coords_key + ":force_flushing"
            try:
                await client.rename(cls.redis_coords_key, coords_temp)
            except Exception as rename_coords_err:
                if "no such key" in str(rename_coords_err).lower():
                    coords_temp = None  # proceed without coords
                else:
                    raise

            # read data
            lines = await client.lrange(main_temp, 0, -1)
            if not lines:
                logger.debug("📭 No Pokémon IV list data in force-flush buffer")
                return 0

            total_lines = len(lines)

            # coords map
            coords_map = {}
            if coords_temp:
                coords_raw = await client.hgetall(coords_temp)
                for k, v in coords_raw.items():
                    try:
                        sp = k.decode() if isinstance(k, (bytes, bytearray)) else k
                        s  = v.decode() if isinstance(v, (bytes, bytearray)) else v
                        la, lo = s.split(",", 1)
                        coords_map[sp] = (float(la), float(lo))
                    except Exception:
                        pass

            # build batch
            data_batch = []
            malformed = 0
            used_coords = 0
            missing_coords = 0

            for b in lines:
                try:
                    line = b.decode() if isinstance(b, (bytes, bytearray)) else b
                    sp_hex, pid_s, form_s, iv_s, level_s, area_s, fs_s = line.split("|")
                    lat, lon = coords_map.get(sp_hex, (None, None))
                    if lat is None or lon is None:
                        missing_coords += 1
                    else:
                        used_coords += 1

                    data_batch.append({
                        "spawnpoint": sp_hex,
                        "pokemon_id": int(pid_s),
                        "form": form_s,
                        "iv": int(iv_s),
                        "level": int(level_s),
                        "area_id": int(area_s),
                        "first_seen": int(fs_s),
                        "latitude": lat,
                        "longitude": lon,
                    })
                except Exception:
                    malformed += 1

            logger.info(
                f"📤 FORCE flushing IV events: batch={len(data_batch)}, "
                f"coords_used={used_coords}, coords_missing={missing_coords}, malformed={malformed}"
            )

            if not data_batch:
                return 0

            inserted_count = await PokemonSQLProcessor.bulk_insert_iv_daily_events(data_batch)
            logger.success(f"📬 FORCE IV events inserted +{inserted_count} rows into MySQL.")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Pokémon IV force-flush failed: {e}", exc_info=True)
            return 0
        finally:
            # cleanup
            try:
                if main_temp and client:
                    await client.delete(main_temp)
            except Exception:
                pass
            try:
                if coords_temp and client:
                    await client.delete(coords_temp)
            except Exception:
                pass


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
            if None in [username, pokemon_id, area_id, first_seen]:
                logger.warning("❌ Shiny event missing required fields. Skipping.")
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

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        """
        Flush buffered shiny aggregates into MySQL.
        Returns the number of aggregated rows consumed by SQL.
        """
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Shiny flush: No Redis connection available after retries.")
            return 0

        temp_key = None
        try:
            if not await client.exists(cls.redis_key):
                logger.debug("📭 No aggregated shiny data to flush.")
                return 0

            # 1) rename to temp
            temp_key = cls.redis_key + ":flushing"
            try:
                await client.rename(cls.redis_key, temp_key)
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("📭 Shiny hash disappeared before rename. Nothing to flush.")
                    return 0
                raise

            # 2) read
            aggregated_data = await client.hgetall(temp_key)
            if not aggregated_data:
                logger.debug("📭 No shiny data in temporary hash; skipping.")
                return 0

            total_keys = len(aggregated_data)
            formatted_data = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): int(v)
                for k, v in aggregated_data.items()
            }

            # 3) build batch
            data_batch = []
            malformed = 0
            for composite_key, count in formatted_data.items():
                try:
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
                        "increment": int(count),
                    })
                except Exception:
                    malformed += 1

            logger.info(
                f"📤 Flushing shiny rates: keys={total_keys}, batch={len(data_batch)}, malformed={malformed}"
            )

            if not data_batch:
                return 0

            # 4) upsert
            inserted_count = await PokemonSQLProcessor.bulk_upsert_shiny_username_rate_batch(data_batch)
            logger.success(f"✨ Shiny rates upserted +{inserted_count} rows into MySQL.")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Error during shiny buffer flush: {e}", exc_info=True)
            return 0
        finally:
            # 5) cleanup
            try:
                if temp_key and client:
                    await client.delete(temp_key)
            except Exception:
                pass

    @classmethod
    async def force_flush(cls, redis_client: Redis):
        """Force flush all buffered shiny data regardless of thresholds."""
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Shiny force-flush: No Redis connection available after retries.")
            return 0

        temp_key = None
        try:
            if not await client.exists(cls.redis_key):
                logger.debug("📭 No shiny data to force flush")
                return 0

            # Atomically rename the buffer key
            temp_key = cls.redis_key + ":force_flushing"
            await client.rename(cls.redis_key, temp_key)

            aggregated_data = await client.hgetall(temp_key)
            if not aggregated_data:
                logger.debug("📭 No shiny data in force-flush buffer")
                await client.delete(temp_key)
                return 0

            # Process data
            formatted_data = {
                (key.decode("utf-8") if isinstance(key, bytes) else key): int(count)
                for key, count in aggregated_data.items()
            }

            data_batch = []
            for composite_key, count in formatted_data.items():
                parts = composite_key.split("|")
                if len(parts) != 6:
                    continue

                data_batch.append({
                    "username": parts[0],
                    "pokemon_id": int(parts[1]),
                    "form": parts[2],
                    "shiny": int(parts[3]),
                    "area_id": int(parts[4]),
                    "first_seen": int(datetime.strptime(parts[5], "%y%m").timestamp()),
                    "increment": count
                })

            inserted_count = await PokemonSQLProcessor.bulk_upsert_shiny_username_rate_batch(data_batch)
            logger.debug(f"🔚 Force-flushed {inserted_count} shiny records")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Shiny rate force-flush failed: {e}")
            return 0
        finally:
            try:
                if temp_key and client:
                    await client.delete(temp_key)
            except Exception:
                pass
