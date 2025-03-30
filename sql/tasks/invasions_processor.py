import asyncio
import random
import aiomysql
from datetime import datetime
import time
from utils.logger import logger
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

class InvasionSQLProcessor:
    @classmethod
    async def upsert_aggregated_invasion_from_filtered(cls, filtered_data, increment: int = 1, pool=None):
        """
        Process a single invasion record with proper error handling and retry logic.
        """
        if pool is None:
            pool = await get_mysql_pool()

        try:
            # Validate required fields
            pokestop_id = filtered_data.get('invasion_pokestop_id')
            if not pokestop_id:
                logger.warning("⚠️ Missing pokestop_id in invasion data")
                return 0

            # Validate coordinates
            try:
                latitude = float(filtered_data.get('invasion_latitude', 0))
                longitude = float(filtered_data.get('invasion_longitude', 0))
            except (ValueError, TypeError):
                logger.warning(f"⚠️ Invalid coordinates for pokestop {pokestop_id}")
                return 0

            area_id = filtered_data.get('area_id')
            if not area_id:
                logger.warning(f"⚠️ Missing area_id for pokestop {pokestop_id}")
                return 0

            # Validate invasion-specific fields
            display_type = filtered_data.get('invasion_type', 0)
            character = filtered_data.get('invasion_character', 0)
            grunt = filtered_data.get('invasion_grunt_type', 0)
            confirmed = filtered_data.get('invasion_confirmed', 0)

            # Validate timestamp
            try:
                first_seen = filtered_data.get('invasion_first_seen')
                if not first_seen:
                    logger.warning(f"⚠️ Missing first_seen for pokestop {pokestop_id}")
                    return 0
                dt = datetime.fromtimestamp(first_seen)
                month_year = int(dt.strftime("%y%m"))
            except (ValueError, TypeError):
                logger.warning(f"⚠️ Invalid first_seen timestamp for pokestop {pokestop_id}")
                return 0

            # Process the record with retry logic
            start_time = time.perf_counter()
            success = await cls._upsert_invasion_with_retry(
                pool=pool,
                pokestop_id=pokestop_id,
                pokestop_name=filtered_data.get('invasion_pokestop_name', ''),
                latitude=latitude,
                longitude=longitude,
                display_type=display_type,
                character=character,
                grunt=grunt,
                confirmed=confirmed,
                area_id=area_id,
                month_year=month_year,
                increment=increment,
                max_retries=10
            )
            processing_time = time.perf_counter() - start_time

            if success:
                logger.debug(f"✅ Processed invasion for pokestop {pokestop_id} in area {area_id} in {processing_time:.4f}s")
                return 1
            logger.warning(f"⚠️ Failed to process invasion for pokestop {pokestop_id} after {processing_time:.4f}s")
            return 0

        except Exception as e:
            logger.error(f"❌ Unexpected error processing invasion for pokestop {pokestop_id}: {e}", exc_info=True)
            return 0

    @classmethod
    async def _upsert_invasion_with_retry(cls, pool, **invasion_data):
        """
        Helper method to handle the actual upsert operation with retry logic.
        """
        increment = invasion_data.get('increment', 1)
        for attempt in range(invasion_data['max_retries']):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        # Time pokestop upsert
                        pokestop_start = time.perf_counter()
                        await cursor.execute(
                            """
                            INSERT INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                pokestop_name = VALUES(pokestop_name),
                                latitude = VALUES(latitude),
                                longitude = VALUES(longitude)
                            """,
                            (
                                invasion_data['pokestop_id'],
                                invasion_data['pokestop_name'],
                                invasion_data['latitude'],
                                invasion_data['longitude']
                            )
                        )
                        pokestop_time = time.perf_counter() - pokestop_start

                        # Time invasion upsert
                        invasion_start = time.perf_counter()
                        await cursor.execute(
                            """
                            INSERT INTO aggregated_invasions (
                                pokestop_id, display_type, `character`, grunt, confirmed,
                                area_id, month_year, total_count
                            )
                            SELECT
                                p.id, %s, %s, %s, %s, %s, %s, %s
                            FROM pokestops p
                            WHERE p.pokestop = %s
                            ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)
                            """,
                            (
                                invasion_data['display_type'],
                                invasion_data['character'],
                                invasion_data['grunt'],
                                invasion_data['confirmed'],
                                invasion_data['area_id'],
                                invasion_data['month_year'],
                                increment,
                                invasion_data['pokestop_id']
                            )
                        )
                        invasion_time = time.perf_counter() - invasion_start

                        await conn.commit()
                        logger.debug(f"⏱️ DB ops timing - Pokestop: {pokestop_time:.4f}s, Invasion: {invasion_time:.4f}s")
                        return True

            except aiomysql.Error as e:
                if e.args[0] == 1213:  # Deadlock error code
                    wait = random.uniform(0.1, 0.5)
                    logger.warning(f"⚠️ Deadlock detected processing invasion. Retrying ({attempt+1}/{invasion_data['max_retries']}) in {wait:.2f}s...")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"❌ Database error processing invasion: {e}", exc_info=True)
                    return False
            except Exception as e:
                logger.error(f"❌ Unexpected error processing invasion: {e}", exc_info=True)
                return False

        logger.error("❌ Max retries reached for invasion upsert")
        return False
