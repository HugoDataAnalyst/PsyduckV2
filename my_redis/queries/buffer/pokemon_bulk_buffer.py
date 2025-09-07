import json
import asyncio
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.calc_iv_bucket import get_iv_bucket
from datetime import datetime
from sql.tasks.pokemon_processor import PokemonSQLProcessor
import config as AppConfig

class PokemonIVRedisBuffer:
    redis_key = "buffer:agg_pokemon_iv"
    redis_coords_key = "buffer:agg_pokemon_iv:coords"
    aggregation_threshold = int(AppConfig.pokemon_max_threshold)

    @classmethod
    async def increment_event(cls, redis_client: Redis, event_data: dict):
        try:
            # Construct a unique key based on your event fields
            spawnpoint = event_data.get("spawnpoint")
            pokemon_id = event_data.get("pokemon_id")
            form = event_data.get("form", 0)
            raw_iv = event_data.get("iv")
            area_id = event_data.get("area_id")
            first_seen = event_data.get("first_seen")
            latitude = event_data.get("latitude")
            longitude = event_data.get("longitude")
            if None in [spawnpoint, pokemon_id, raw_iv, area_id, first_seen]:
                logger.warning("❌ Event missing required fields. Skipping.")
                return

            bucket_iv = get_iv_bucket(raw_iv)
            dt = datetime.fromtimestamp(first_seen)
            month_year = dt.strftime("%y%m")  # e.g. "2503" for March 2025

            # Create a composite unique key string
            unique_key = f"{spawnpoint}_{pokemon_id}_{form}_{bucket_iv}_{area_id}_{month_year}"

            # Persist coordinates once per spawnpoint
            if latitude is not None and longitude is not None:
                try:
                    await redis_client.hsetnx(cls.redis_coords_key, spawnpoint, f"{latitude},{longitude}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to cache coords for spawnpoint {spawnpoint}: {e}")

            # Increment the count for this unique combination and log the new count for this key.
            new_count = await redis_client.hincrby(cls.redis_key, unique_key, 1)
            logger.debug(f"Incremented key '{unique_key}' to {new_count}.")

            # Check total unique keys in the hash
            current_unique_count = await redis_client.hlen(cls.redis_key)
            logger.debug(f"📊 👻 Current total unique aggregated IV keys: {current_unique_count}")

            # Flush if the number of unique combinations exceeds the threshold
            if current_unique_count >= cls.aggregation_threshold:
                logger.warning(f"📊 👻 Aggregation threshold reached: {current_unique_count} unique keys. Initiating flush...")
                await cls.flush_if_ready(redis_client)
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
        main_temp = None
        coords_temp = None
        try:
            # 0) Quick check
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No aggregated IV data to flush.")
                return 0

            # 1) Atomically rename the main aggregate hash and the companion coords hash
            main_temp = cls.redis_key + ":flushing"            # e.g., buffer:agg_pokemon_iv:flushing
            coords_temp = cls.redis_coords_key + ":flushing"   # e.g., buffer:agg_pokemon_iv:coords:flushing

            try:
                await redis_client.rename(cls.redis_key, main_temp)
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("📭 IV hash disappeared before rename. Nothing to flush.")
                    return 0
                raise

            # coords hash is optional
            try:
                await redis_client.rename(cls.redis_coords_key, coords_temp)
            except Exception as rename_coords_err:
                if "no such key" in str(rename_coords_err).lower():
                    coords_temp = None  # proceed without coords
                else:
                    raise

            # 2) Read both hashes
            aggregated_data = await redis_client.hgetall(main_temp)
            if not aggregated_data:
                logger.debug("📭 No IV data in temporary hash; skipping.")
                return 0

            total_keys = len(aggregated_data)

            formatted_data: dict[str, int] = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): int(v)
                for k, v in aggregated_data.items()
            }

            coords_map: dict[str, tuple[float, float]] = {}
            if coords_temp:
                coords_raw = await redis_client.hgetall(coords_temp)
                if coords_raw:
                    for k, v in coords_raw.items():
                        try:
                            sp_hex = k.decode() if isinstance(k, bytes) else k
                            val = v.decode() if isinstance(v, bytes) else v
                            lat_str, lon_str = val.split(",", 1)
                            coords_map[sp_hex] = (float(lat_str), float(lon_str))
                        except Exception:
                            # ignore malformed coord entries
                            pass

            # 3) Build the SQL batch
            data_batch = []
            malformed = 0
            used_coords = 0
            missing_coords = 0

            for composite_key, count in formatted_data.items():
                # expected: spawnpoint_hex_pokemonId_form_bucketIV_areaId_YYMM
                try:
                    parts = composite_key.split("_")
                    if len(parts) != 6:
                        malformed += 1
                        continue

                    sp_hex, pid_s, form_s, bucket_s, area_s, yymm_s = parts

                    lat, lon = coords_map.get(sp_hex, (None, None))
                    if lat is None or lon is None:
                        missing_coords += 1
                    else:
                        used_coords += 1

                    data_batch.append({
                        "spawnpoint": sp_hex,  # SQL layer converts hex -> int
                        "latitude": lat,
                        "longitude": lon,
                        "pokemon_id": int(pid_s),
                        "form": form_s,
                        "iv": int(bucket_s),  # already bucketed (0,25,50,75,90,95,100)
                        "area_id": int(area_s),
                        "first_seen": int(datetime.strptime(yymm_s, "%y%m").timestamp()),
                        "increment": int(count),
                    })
                except Exception:
                    malformed += 1

            logger.info(
                f"📤 Flushing IV heatmap: keys={total_keys}, batch={len(data_batch)}, "
                f"coords_used={used_coords}, coords_missing={missing_coords}, malformed={malformed}"
            )

            if not data_batch:
                return 0

            # 4) Upsert into SQL
            inserted_count = await PokemonSQLProcessor.bulk_upsert_aggregated_pokemon_iv_monthly_batch_v2(data_batch)
            logger.success(f"📬 IV heatmap upserted +{inserted_count} rows into MySQL.")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Error during IV buffer flush: {e}", exc_info=True)
            return 0
        finally:
            # 5) Cleanup temp keys
            try:
                if main_temp:
                    await redis_client.delete(main_temp)
            except Exception:
                pass
            try:
                if coords_temp:
                    await redis_client.delete(coords_temp)
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
        main_temp = None
        coords_temp = None
        try:
            # quick existence check
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No Pokémon IV data to force flush")
                return 0

            # rename main to a force temp
            main_temp = cls.redis_key + ":force_flushing"
            try:
                await redis_client.rename(cls.redis_key, main_temp)
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("📭 IV hash disappeared before force rename. Nothing to flush.")
                    return 0
                raise

            # rename coords hash if present
            coords_temp = cls.redis_coords_key + ":force_flushing"
            try:
                await redis_client.rename(cls.redis_coords_key, coords_temp)
            except Exception as rename_coords_err:
                if "no such key" in str(rename_coords_err).lower():
                    coords_temp = None  # proceed without coords
                else:
                    raise

            # read data
            aggregated_data = await redis_client.hgetall(main_temp)
            if not aggregated_data:
                logger.debug("📭 No Pokémon IV data in force-flush buffer")
                return 0

            total_keys = len(aggregated_data)
            formatted_data = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): int(v)
                for k, v in aggregated_data.items()
            }

            # coords map
            coords_map: dict[str, tuple[float, float]] = {}
            if coords_temp:
                coords_raw = await redis_client.hgetall(coords_temp)
                if coords_raw:
                    for k, v in coords_raw.items():
                        try:
                            sp_hex = k.decode() if isinstance(k, bytes) else k
                            val = v.decode() if isinstance(v, bytes) else v
                            lat_str, lon_str = val.split(",", 1)
                            coords_map[sp_hex] = (float(lat_str), float(lon_str))
                        except Exception:
                            # ignore malformed coord entries
                            pass

            # build batch
            data_batch = []
            malformed = 0
            used_coords = 0
            missing_coords = 0

            for composite_key, count in formatted_data.items():
                try:
                    # expected format: sp_hex_pid_form_bucket_area_yymm  (6 parts)
                    parts = composite_key.split("_")
                    if len(parts) != 6:
                        malformed += 1
                        continue

                    sp_hex, pid_s, form_s, bucket_s, area_s, yymm_s = parts

                    lat, lon = coords_map.get(sp_hex, (None, None))
                    if lat is None or lon is None:
                        missing_coords += 1
                    else:
                        used_coords += 1

                    data_batch.append({
                        "spawnpoint": sp_hex,
                        "latitude": lat,
                        "longitude": lon,
                        "pokemon_id": int(pid_s),
                        "form": form_s,
                        "iv": int(bucket_s),  # already bucketed (0,25,50,75,90,95,100)
                        "area_id": int(area_s),
                        "first_seen": int(datetime.strptime(yymm_s, "%y%m").timestamp()),
                        "increment": int(count),
                    })
                except Exception:
                    malformed += 1

            logger.info(
                f"📤 FORCE flushing IV heatmap: keys={total_keys}, batch={len(data_batch)}, "
                f"coords_used={used_coords}, coords_missing={missing_coords}, malformed={malformed}"
            )

            if not data_batch:
                return 0

            inserted_count = await PokemonSQLProcessor.bulk_upsert_aggregated_pokemon_iv_monthly_batch_v2(data_batch)
            logger.success(f"📬 FORCE IV heatmap upserted +{inserted_count} rows into MySQL.")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Pokémon IV force-flush failed: {e}", exc_info=True)
            return 0
        finally:
            # cleanup
            try:
                if main_temp:
                    await redis_client.delete(main_temp)
            except Exception:
                pass
            try:
                if coords_temp:
                    await redis_client.delete(coords_temp)
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
            shiny = int(event_data.get("shiny", 0))
            area_id = event_data.get("area_id")
            first_seen = event_data.get("first_seen")
            if None in [username, pokemon_id, area_id, first_seen]:
                logger.warning("❌ Shiny event missing required fields. Skipping.")
                return

            dt = datetime.fromtimestamp(first_seen)
            month_year = dt.strftime("%y%m")  # e.g. "2503" for March 2025

            # Construct a composite unique key
            unique_key = f"{username}|{pokemon_id}|{form}|{shiny}|{area_id}|{month_year}"

            # Increment the count for this unique key
            new_count = await redis_client.hincrby(cls.redis_key, unique_key, 1)
            logger.debug(f"Incremented shiny key '{unique_key}' to {new_count}.")

            # Check total unique keys in the hash
            current_unique_count = await redis_client.hlen(cls.redis_key)
            logger.debug(f"📊 🌟 Current total unique aggregated shiny keys: {current_unique_count}")

            # Flush if threshold is reached
            if current_unique_count >= cls.aggregation_threshold:
                logger.warning(f"📊 🌟 Shiny aggregation threshold reached: {current_unique_count} unique keys. Initiating flush...")
                await cls.flush_if_ready(redis_client)
        except Exception as e:
            logger.error(f"❌ Error incrementing aggregated shiny event: {e}")

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        """
        Flush buffered shiny aggregates into MySQL.
        Returns the number of aggregated rows consumed by SQL.
        """
        temp_key = None
        try:
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No aggregated shiny data to flush.")
                return 0

            # 1) rename to temp
            temp_key = cls.redis_key + ":flushing"
            try:
                await redis_client.rename(cls.redis_key, temp_key)
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("📭 Shiny hash disappeared before rename. Nothing to flush.")
                    return 0
                raise

            # 2) read
            aggregated_data = await redis_client.hgetall(temp_key)
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
                if temp_key:
                    await redis_client.delete(temp_key)
            except Exception:
                pass

    @classmethod
    async def force_flush(cls, redis_client: Redis):
        """Force flush all buffered shiny data regardless of thresholds."""
        try:
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No shiny data to force flush")
                return 0

            # Atomically rename the buffer key
            temp_key = cls.redis_key + ":force_flushing"
            await redis_client.rename(cls.redis_key, temp_key)

            aggregated_data = await redis_client.hgetall(temp_key)
            if not aggregated_data:
                logger.debug("📭 No shiny data in force-flush buffer")
                await redis_client.delete(temp_key)
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
            await redis_client.delete(temp_key)
            logger.debug(f"🔚 Force-flushed {inserted_count} shiny records")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Shiny rate force-flush failed: {e}")
            return 0
