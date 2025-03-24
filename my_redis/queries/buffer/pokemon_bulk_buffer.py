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
            unique_key = f"{spawnpoint}_{pokemon_id}_{form}_{bucket_iv}_{area_id}_{month_year}_{latitude}_{longitude}"

            # Increment the count for this unique combination and log the new count for this key.
            new_count = await redis_client.hincrby(cls.redis_key, unique_key, 1)
            logger.debug(f"Incremented key '{unique_key}' to {new_count}.")

            # Check total unique keys in the hash
            current_unique_count = await redis_client.hlen(cls.redis_key)
            logger.debug(f"📊 👻 Current total unique aggregated IV keys: {current_unique_count}")

            # Flush if the number of unique combinations exceeds the threshold
            if current_unique_count >= cls.aggregation_threshold:
                logger.info(f"📊 👻 Aggregation threshold reached: {current_unique_count} unique keys. Initiating flush...")
                await cls.flush_if_ready(redis_client)
        except Exception as e:
            logger.error(f"❌ Error incrementing aggregated event: {e}")

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis):
        try:
            # Check if there's any data to flush.
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No aggregated data to flush.")
                return

            # Atomically rename the current hash to a temporary key.
            temp_key = cls.redis_key + ":flushing"
            try:
                await redis_client.rename(cls.redis_key, temp_key)
                logger.debug(f"Renamed {cls.redis_key} to {temp_key} for flushing.")
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("No such key found during rename. Nothing to flush.")
                    return
                else:
                    raise

            # Retrieve all aggregated data from the temporary key.
            aggregated_data = await redis_client.hgetall(temp_key)
            if not aggregated_data:
                logger.debug("📭 No aggregated data to flush from temporary key.")
                return

            # Convert the data from bytes to proper types.
            formatted_data = {
                (key.decode("utf-8") if isinstance(key, bytes) else key): int(count)
                for key, count in aggregated_data.items()
            }

            data_batch = []
            for composite_key, count in formatted_data.items():
                parts = composite_key.split("_")
                if len(parts) != 8:
                    logger.warning(f"Invalid composite key format: {composite_key}")
                    continue
                (spawnpoint_hex, pokemon_id_str, form_str, bucket_iv_str,
                area_id_str, month_year_str, latitude_str, longitude_str) = parts
                data_batch.append({
                    "spawnpoint": spawnpoint_hex,  # The upsert function will convert via int(..., 16)
                    "latitude": float(latitude_str),
                    "longitude": float(longitude_str),
                    "pokemon_id": int(pokemon_id_str),
                    "form": form_str,
                    "iv": int(bucket_iv_str),
                    "area_id": int(area_id_str),
                    "first_seen": int(datetime.strptime(month_year_str, "%y%m").timestamp()),
                    "increment": count
                })

            logger.info(f"📤 Flushing {len(formatted_data)} unique aggregated 👻 Pokémon events from temporary key.")

            # Upsert the aggregated data into SQL.
            inserted_count = await PokemonSQLProcessor.bulk_upsert_aggregated_pokemon_iv_monthly_batch(data_batch)
            logger.success(f"📬 Inserted {inserted_count} aggregated Pokémon IV rows.")

            # Delete the temporary key.
            await redis_client.delete(temp_key)
        except Exception as e:
            logger.error(f"❌ Error during aggregated buffer flush: {e}", exc_info=True)


    @classmethod
    async def force_flush(cls, redis_client: Redis):
        """Force flush all buffered data regardless of thresholds."""
        try:
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No Pokémon IV data to force flush")
                return 0

            # Atomically rename the buffer key
            temp_key = cls.redis_key + ":force_flushing"
            await redis_client.rename(cls.redis_key, temp_key)

            aggregated_data = await redis_client.hgetall(temp_key)
            if not aggregated_data:
                logger.debug("📭 No Pokémon IV data in force-flush buffer")
                await redis_client.delete(temp_key)
                return 0

            # Process data (same as flush_if_ready)
            formatted_data = {
                (key.decode("utf-8") if isinstance(key, bytes) else key): int(count)
                for key, count in aggregated_data.items()
            }

            data_batch = []
            for composite_key, count in formatted_data.items():
                parts = composite_key.split("_")
                if len(parts) != 8:
                    continue
                # ... [same parsing logic as flush_if_ready]
                data_batch.append({
                    "spawnpoint": parts[0],
                    "latitude": float(parts[6]),
                    "longitude": float(parts[7]),
                    "pokemon_id": int(parts[1]),
                    "form": parts[2],
                    "iv": int(parts[3]),
                    "area_id": int(parts[4]),
                    "first_seen": int(datetime.strptime(parts[5], "%y%m").timestamp()),
                    "increment": count
                })

            inserted_count = await PokemonSQLProcessor.bulk_upsert_aggregated_pokemon_iv_monthly_batch(data_batch)
            await redis_client.delete(temp_key)
            logger.debug(f"🔚 Force-flushed {inserted_count} Pokémon IV records")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ Pokémon IV force-flush failed: {e}")
            return 0


class ShinyRateRedisBuffer:
    redis_key = "buffer:agg_shiny_rates_hash"
    aggregation_threshold = int(AppConfig.shiny_max_threshold)  # adjust as needed

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
            unique_key = f"{username}_{pokemon_id}_{form}_{shiny}_{area_id}_{month_year}"

            # Increment the count for this unique key
            new_count = await redis_client.hincrby(cls.redis_key, unique_key, 1)
            logger.debug(f"Incremented shiny key '{unique_key}' to {new_count}.")

            # Check total unique keys in the hash
            current_unique_count = await redis_client.hlen(cls.redis_key)
            logger.debug(f"📊 🌟 Current total unique aggregated shiny keys: {current_unique_count}")

            # Flush if threshold is reached
            if current_unique_count >= cls.aggregation_threshold:
                logger.info(f"📊 🌟 Shiny aggregation threshold reached: {current_unique_count} unique keys. Initiating flush...")
                await cls.flush_if_ready(redis_client)
        except Exception as e:
            logger.error(f"❌ Error incrementing aggregated shiny event: {e}")

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis):
        try:
            # Check if the hash exists; if not, nothing to flush.
            if not await redis_client.exists(cls.redis_key):
                logger.debug("📭 No aggregated shiny data to flush.")
                return

            # Atomically rename the current hash to a temporary key.
            temp_key = cls.redis_key + ":flushing"
            try:
                await redis_client.rename(cls.redis_key, temp_key)
                logger.debug(f"Renamed {cls.redis_key} to {temp_key} for flushing.")
            except Exception as rename_err:
                if "no such key" in str(rename_err).lower():
                    logger.debug("No such key found during rename. Nothing to flush.")
                    return
                else:
                    raise

            # Retrieve all aggregated data from the temporary key.
            aggregated_data = await redis_client.hgetall(temp_key)
            if not aggregated_data:
                logger.debug("📭 No aggregated shiny data to flush from temporary key.")
                return

            # Convert keys/values from bytes to appropriate types.
            formatted_data = {
                (key.decode("utf-8") if isinstance(key, bytes) else key): int(count)
                for key, count in aggregated_data.items()
            }
            logger.info(f"📤 Flushing {len(formatted_data)} unique aggregated 🌟 shiny events from temporary key.")

            # Convert formatted_data into a data batch list.
            data_batch = []
            for composite_key, count in formatted_data.items():
                parts = composite_key.split("_")
                if len(parts) != 6:
                    logger.warning(f"Invalid aggregated key format: {composite_key}")
                    continue
                username, pokemon_id_str, form_str, shiny_str, area_id_str, month_year_str = parts

                data_batch.append({
                    "username": username,
                    "pokemon_id": int(pokemon_id_str),
                    "form": form_str,
                    "shiny": int(shiny_str),  # Bulk upsert function will convert this to int.
                    "area_id": int(area_id_str),
                    "first_seen": int(datetime.strptime(month_year_str, "%y%m").timestamp()),
                    "increment": count
                })

            # Upsert the aggregated shiny rates into SQL.
            inserted_count = await PokemonSQLProcessor.bulk_upsert_shiny_username_rate_batch(data_batch)
            logger.success(f"✨ Inserted {inserted_count} aggregated shiny rate rows.")

            # Delete the temporary key after successful flush.
            await redis_client.delete(temp_key)
        except Exception as e:
            logger.error(f"❌ Error during aggregated shiny buffer flush: {e}", exc_info=True)


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

            # Process data (same as flush_if_ready)
            formatted_data = {
                (key.decode("utf-8") if isinstance(key, bytes) else key): int(count)
                for key, count in aggregated_data.items()
            }

            data_batch = []
            for composite_key, count in formatted_data.items():
                parts = composite_key.split("_")
                if len(parts) != 6:
                    continue
                # ... [same parsing logic as flush_if_ready]
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
