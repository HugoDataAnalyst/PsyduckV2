import asyncio
import random
import aiomysql
from datetime import datetime
from utils.logger import logger
from utils.calc_iv_bucket import get_iv_bucket
import config as AppConfig

async def get_mysql_pool():
    pool = await aiomysql.create_pool(
        host=AppConfig.db_host,
        port=AppConfig.db_port,
        user=AppConfig.db_user,
        password=AppConfig.db_password,
        db=AppConfig.db_name,
        autocommit=True,
        loop=asyncio.get_running_loop()
    )
    return pool

class PokemonSQLProcessor:
    @classmethod
    async def bulk_upsert_aggregated_pokemon_iv_monthly_batch(cls, data_batch: list, pool=None, max_retries=10) -> int:
        """
        Batch upsert aggregated IV data using a raw SQL query with ON DUPLICATE KEY UPDATE.
        This method:
        1. Extracts spawnpoint data and performs a bulk insert into the spawnpoints table using INSERT IGNORE.
        2. Builds a list of tuples for the aggregated upsert.
        3. Uses executemany to perform a batch upsert.

        Assumes the AggregatedPokemonIVMonthly table has a unique constraint on:
        (spawnpoint, pokemon_id, form, iv, area_id, month_year)
        """

        pool = None
        # Prepare lists for spawnpoint upsert and aggregated data.
        spawnpoints = {}  # Mapping: spawnpoint_value -> (latitude, longitude)
        aggregated_values = []  # List of tuples for bulk upsert

        try:
            pool = await get_mysql_pool()
            for data in data_batch:
                try:
                    # Skip if spawnpoint is None or not a valid hex string.
                    if data['spawnpoint'] is None:
                        logger.warning("⚠️ Spawnpoint is None; skipping record.")
                        continue

                    try:
                        # Convert spawnpoint hex string to int.
                        spawnpoint_value = int(data['spawnpoint'], 16)
                    except ValueError as e:
                        logger.warning(f"⚠️ Invalid spawnpoint hex value '{data['spawnpoint']}'; skipping record.")
                        continue

                    # Ensure latitude and longitude are valid floats.
                    try:
                        latitude = float(data['latitude'])
                        longitude = float(data['longitude'])
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Invalid latitude or longitude; skipping record.")
                        continue

                    spawnpoints[spawnpoint_value] = (latitude, longitude)

                    # Convert raw IV to bucket.
                    bucket_iv = get_iv_bucket(data['iv'])
                    if bucket_iv is None:
                        logger.warning("⚠️ Bucket conversion returned None; skipping record.")
                        continue

                    # Convert first_seen to month_year.
                    try:
                        dt = datetime.fromtimestamp(data['first_seen'])
                        month_year = int(dt.strftime("%y%m"))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Invalid first_seen timestamp; skipping record.")
                        continue

                    # Use provided increment or default to 1.
                    increment = data.get('increment', 1)

                    aggregated_values.append((
                        spawnpoint_value,
                        data['pokemon_id'],
                        data.get('form', 0),
                        bucket_iv,
                        data['area_id'],
                        month_year,
                        increment
                    ))
                except Exception as e:
                    logger.error(f"❌ Error processing data batch record: {e}", exc_info=True)

            if not aggregated_values:
                logger.warning("⚠️ No valid aggregated records to upsert.")
                return 0

            # Step 1: Bulk insert spawnpoints with deadlock handling.
            spawnpoint_values = [
                (sp, lat, lon) for sp, (lat, lon) in spawnpoints.items()
            ]
            spawnpoint_success = await cls._bulk_upsert_with_retry(
                pool,
                "spawnpoints",
                """
                INSERT INTO spawnpoints (spawnpoint, latitude, longitude)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE latitude = VALUES(latitude), longitude = VALUES(longitude);
                """,
                spawnpoint_values,
                max_retries,
                "spawnpoints"
            )
            if not spawnpoint_success:
                logger.error("❌ Failed to upsert spawnpoints after retries.")
                return 0

            # Step 2: Bulk upsert aggregated IV data with deadlock handling.
            aggregated_success = await cls._bulk_upsert_with_retry(
                pool,
                "aggregated_pokemon_iv_monthly",
                """
                INSERT INTO aggregated_pokemon_iv_monthly
                (spawnpoint, pokemon_id, form, iv, area_id, month_year, total_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)
                """,
                aggregated_values,
                max_retries,
                "aggregated Pokémon IV"
            )
            if not aggregated_success:
                logger.error("❌ Failed to upsert aggregated Pokémon IV data after retries.")
                return 0

            return len(aggregated_values)

        finally:
            if pool is not None:
                pool.close()
                await pool.wait_closed()


    @classmethod
    async def _bulk_upsert_with_retry(cls, pool, table_name, sql, values, max_retries, operation_name):
        """
        Helper method to perform bulk upsert with deadlock and lock timeout retries.
        Handles both deadlocks (1213) and lock wait timeouts (1205).
        """
        for attempt in range(max_retries):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        # Set a more aggressive timeout for this transaction
                        await cursor.execute("SET SESSION innodb_lock_wait_timeout = 10")

                        # Execute the main operation
                        await cursor.executemany(sql, values)
                        await conn.commit()
                        logger.debug(f"✅ Bulk upserted {len(values)} {operation_name} rows.")
                        return True

            except aiomysql.Error as e:
                if e.args[0] in (1213, 1205):  # Deadlock (1213) or Lock timeout (1205)
                    wait_time = random.uniform(0.5, 2.0) * (attempt + 1)  # Exponential backoff
                    logger.warning(
                        f"⚠️ {'Deadlock' if e.args[0] == 1213 else 'Lock timeout'} detected for {table_name}. "
                        f"Retrying ({attempt + 1}/{max_retries}) in {wait_time:.2f}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ Error during {table_name} bulk upsert: {e}", exc_info=True)
                    return False

            except Exception as e:
                logger.error(f"❌ Unexpected error during {table_name} bulk upsert: {e}", exc_info=True)
                return False

        logger.error(f"❌ Max retries reached for {table_name}. Failed to upsert data.")
        return False

    @classmethod
    async def bulk_upsert_shiny_username_rate_batch(cls, data_batch: list, pool=None, max_retries=10) -> int:
        """
        Batch upsert shiny username rate data using a raw SQL query with ON DUPLICATE KEY UPDATE.
        Assumes the ShinyUsernameRates table has a unique constraint on:
          (username, pokemon_id, form, shiny, area_id, month_year)
        """
        pool = None

        aggregated_values = []  # List of tuples for bulk upsert

        try:
            pool = await get_mysql_pool()
            for data in data_batch:
                try:
                    dt = datetime.fromtimestamp(data['first_seen'])
                    month_year = int(dt.strftime("%y%m"))
                    increment = data.get('increment', 1)
                    aggregated_values.append((
                        data['username'],
                        data['pokemon_id'],
                        data.get('form', 0),
                        int(data['shiny']),
                        data['area_id'],
                        month_year,
                        increment
                    ))
                except Exception as e:
                    logger.error(f"❌ Error processing shiny record: {e}", exc_info=True)

            if not aggregated_values:
                logger.warning("⚠️ No valid aggregated shiny records to upsert.")
                return 0

            success = await cls._bulk_upsert_with_retry(
                pool,
                "shiny_username_rates",
                """
                INSERT INTO shiny_username_rates
                (username, pokemon_id, form, shiny, area_id, month_year, total_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)
                """,
                aggregated_values,
                max_retries,
                "shiny username rates"
            )
            if not success:
                logger.error("❌ Failed to upsert shiny username rates after retries.")
                return 0

            return len(aggregated_values)

        finally:
            if pool is not None:
                pool.close()
                await pool.wait_closed()
