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

class RaidSQLProcessor:
    @classmethod
    async def upsert_aggregated_raid_from_filtered(cls, filtered_data, increment: int = 1, pool=None):
        """
        Process a single raid record with proper error handling and retry logic.
        """
        if pool is None:
            pool = await get_mysql_pool()

        try:
            # Validate required fields
            gym_id = filtered_data.get('raid_gym_id')
            if not gym_id:
                logger.warning("Missing gym_id in raid data")
                return 0

            # Validate coordinates
            try:
                latitude = float(filtered_data.get('raid_latitude', 0))
                longitude = float(filtered_data.get('raid_longitude', 0))
            except (ValueError, TypeError):
                logger.warning(f"Invalid coordinates for gym {gym_id}")
                return 0

            area_id = filtered_data.get('area_id')
            if not area_id:
                logger.warning(f"Missing area_id for gym {gym_id}")
                return 0

            # Validate raid-specific fields
            raid_pokemon = filtered_data.get('raid_pokemon', 0)
            raid_level = filtered_data.get('raid_level', 0)
            raid_form = filtered_data.get('raid_form', '0')
            raid_team = filtered_data.get('raid_team_id', 0)
            raid_costume = filtered_data.get('raid_costume', '0')
            raid_is_exclusive = int(filtered_data.get('raid_is_exclusive', 0))
            raid_ex_raid_eligible = int(filtered_data.get('raid_ex_raid_eligible', 0))

            # Validate timestamp
            try:
                first_seen = filtered_data.get('raid_first_seen')
                if not first_seen:
                    logger.warning(f"Missing first_seen for gym {gym_id}")
                    return 0
                dt = datetime.fromtimestamp(first_seen)
                month_year = int(dt.strftime("%y%m"))
            except (ValueError, TypeError):
                logger.warning(f"Invalid first_seen timestamp for gym {gym_id}")
                return 0

            # Process the record with retry logic
            start_time = time.perf_counter()
            success = await cls._upsert_raid_with_retry(
                pool=pool,
                gym_id=gym_id,
                gym_name=filtered_data.get('raid_gym_name', ''),
                latitude=latitude,
                longitude=longitude,
                raid_pokemon=raid_pokemon,
                raid_level=raid_level,
                raid_form=raid_form,
                raid_team=raid_team,
                raid_costume=raid_costume,
                raid_is_exclusive=raid_is_exclusive,
                raid_ex_raid_eligible=raid_ex_raid_eligible,
                area_id=area_id,
                month_year=month_year,
                increment=increment,
                max_retries=10
            )
            processing_time = time.perf_counter() - start_time

            if success:
                logger.debug(f"✅ Processed raid for gym {gym_id} in area {area_id} in {processing_time:.4f}s")
                return 1
            logger.warning(f"⚠️ Failed to process raid for gym {gym_id} after {processing_time:.4f}s")
            return 0

        except Exception as e:
            logger.error(f"❌ Unexpected error processing raid for gym {gym_id}: {e}", exc_info=True)
            return 0

    @classmethod
    async def _upsert_raid_with_retry(cls, pool, **raid_data):
        """
        Helper method to handle the actual upsert operation with retry logic.
        """
        for attempt in range(raid_data['max_retries']):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        # Time gym upsert
                        gym_start = time.perf_counter()
                        await cursor.execute(
                            """
                            INSERT INTO gyms (gym, gym_name, latitude, longitude)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                gym_name = VALUES(gym_name),
                                latitude = VALUES(latitude),
                                longitude = VALUES(longitude)
                            """,
                            (
                                raid_data['gym_id'],
                                raid_data['gym_name'],
                                raid_data['latitude'],
                                raid_data['longitude']
                            )
                        )
                        gym_time = time.perf_counter() - gym_start

                        # Time raid upsert
                        raid_start = time.perf_counter()
                        await cursor.execute(
                            """
                            INSERT INTO aggregated_raids (
                                gym_id, raid_pokemon, raid_level, raid_form, raid_team,
                                raid_costume, raid_is_exclusive, raid_ex_raid_eligible,
                                area_id, month_year, total_count
                            )
                            SELECT
                                g.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            FROM gyms g
                            WHERE g.gym = %s
                            ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)
                            """,
                            (
                                raid_data['raid_pokemon'],
                                raid_data['raid_level'],
                                raid_data['raid_form'],
                                raid_data['raid_team'],
                                raid_data['raid_costume'],
                                raid_data['raid_is_exclusive'],
                                raid_data['raid_ex_raid_eligible'],
                                raid_data['area_id'],
                                raid_data['month_year'],
                                raid_data['increment'],
                                raid_data['gym_id']
                            )
                        )
                        raid_time = time.perf_counter() - raid_start

                        await conn.commit()
                        logger.debug(f"⏱️ DB ops timing - Gym: {gym_time:.4f}s, Raid: {raid_time:.4f}s")
                        return True

            except aiomysql.Error as e:
                if e.args[0] == 1213:  # Deadlock error code
                    wait = random.uniform(0.1, 0.5)
                    logger.warning(f"⚠️ Deadlock detected processing raid. Retrying ({attempt+1}/{raid_data['max_retries']}) in {wait:.2f}s...")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"❌ Database error processing raid: {e}", exc_info=True)
                    return False
            except Exception as e:
                logger.error(f"❌ Unexpected error processing raid: {e}", exc_info=True)
                return False

        logger.error("❌ Max retries reached for raid upsert")
        return False
