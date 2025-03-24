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

class QuestSQLProcessor:
    @classmethod
    async def upsert_aggregated_quest_from_filtered(cls, filtered_data, increment: int = 1, pool=None):
        """
        Process a single quest record with proper error handling and retry logic.
        """
        if pool is None:
            pool = await get_mysql_pool()

        try:
            # Validate required fields
            pokestop_id = filtered_data.get('pokestop_id')
            if not pokestop_id:
                logger.warning("Missing pokestop_id in quest data")
                return 0

            # Validate coordinates
            try:
                latitude = float(filtered_data.get('latitude', 0))
                longitude = float(filtered_data.get('longitude', 0))
            except (ValueError, TypeError):
                logger.warning(f"Invalid coordinates for pokestop {pokestop_id}")
                return 0

            area_id = filtered_data.get('area_id')
            if not area_id:
                logger.warning(f"Missing area_id for pokestop {pokestop_id}")
                return 0

            # Validate timestamp
            try:
                first_seen = filtered_data.get('first_seen')
                if not first_seen:
                    logger.warning(f"Missing first_seen for pokestop {pokestop_id}")
                    return 0
                dt = datetime.fromtimestamp(first_seen)
                month_year = int(dt.strftime("%y%m"))
            except (ValueError, TypeError):
                logger.warning(f"Invalid first_seen timestamp for pokestop {pokestop_id}")
                return 0

            # Process the record with retry logic
            start_time = time.perf_counter()
            success = await cls._upsert_quest_with_retry(
                pool=pool,
                pokestop_id=pokestop_id,
                pokestop_name=filtered_data.get('pokestop_name', ''),
                latitude=latitude,
                longitude=longitude,
                ar_type=filtered_data.get('ar_type', 0),
                normal_type=filtered_data.get('normal_type', 0),
                reward_ar_type=filtered_data.get('reward_ar_type', 0),
                reward_normal_type=filtered_data.get('reward_normal_type', 0),
                reward_ar_item_id=filtered_data.get('reward_ar_item_id', 0),
                reward_ar_item_amount=filtered_data.get('reward_ar_item_amount', 0),
                reward_normal_item_id=filtered_data.get('reward_normal_item_id', 0),
                reward_normal_item_amount=filtered_data.get('reward_normal_item_amount', 0),
                reward_ar_poke_id=filtered_data.get('reward_ar_poke_id', 0),
                reward_ar_poke_form=filtered_data.get('reward_ar_poke_form', ''),
                reward_normal_poke_id=filtered_data.get('reward_normal_poke_id', 0),
                reward_normal_poke_form=filtered_data.get('reward_normal_poke_form', ''),
                area_id=area_id,
                month_year=month_year,
                increment=increment,
                max_retries=10
            )
            processing_time = time.perf_counter() - start_time

            if success:
                logger.debug(f"✅ Processed quest for pokestop {pokestop_id} in area {area_id} in {processing_time:.4f}s")
                return 1
            logger.warning(f"⚠️ Failed to process quest for pokestop {pokestop_id} after {processing_time:.4f}s")
            return 0

        except Exception as e:
            logger.error(f"❌ Unexpected error processing quest for pokestop {pokestop_id}: {e}", exc_info=True)
            return 0

    @classmethod
    async def _upsert_quest_with_retry(cls, pool, **quest_data):
        """
        Helper method to handle the actual upsert operation with retry logic.
        """
        increment = quest_data.get('increment', 1)
        for attempt in range(quest_data['max_retries']):
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
                                quest_data['pokestop_id'],
                                quest_data['pokestop_name'],
                                quest_data['latitude'],
                                quest_data['longitude']
                            )
                        )
                        pokestop_time = time.perf_counter() - pokestop_start

                        # Time quest upsert
                        quest_start = time.perf_counter()
                        await cursor.execute(
                            """
                            INSERT INTO aggregated_quests (
                                pokestop_id, ar_type, normal_type, reward_ar_type, reward_normal_type,
                                reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount,
                                reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form,
                                area_id, month_year, total_count
                            )
                            SELECT
                                p.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            FROM pokestops p
                            WHERE p.pokestop = %s
                            ON DUPLICATE KEY UPDATE total_count = total_count + VALUES(total_count)
                            """,
                            (
                                quest_data['ar_type'],
                                quest_data['normal_type'],
                                quest_data['reward_ar_type'],
                                quest_data['reward_normal_type'],
                                quest_data['reward_ar_item_id'],
                                quest_data['reward_ar_item_amount'],
                                quest_data['reward_normal_item_id'],
                                quest_data['reward_normal_item_amount'],
                                quest_data['reward_ar_poke_id'],
                                quest_data['reward_ar_poke_form'],
                                quest_data['reward_normal_poke_id'],
                                quest_data['reward_normal_poke_form'],
                                quest_data['area_id'],
                                quest_data['month_year'],
                                increment,
                                quest_data['pokestop_id']
                            )
                        )
                        quest_time = time.perf_counter() - quest_start

                        await conn.commit()
                        logger.debug(f"⏱️ DB ops timing - Pokestop: {pokestop_time:.4f}s, Quest: {quest_time:.4f}s")
                        return True

            except aiomysql.Error as e:
                if e.args[0] == 1213:  # Deadlock error code
                    wait = random.uniform(0.1, 0.5)
                    logger.warning(f"⚠️ Deadlock detected processing quest. Retrying ({attempt+1}/{quest_data['max_retries']}) in {wait:.2f}s...")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"❌ Database error processing quest: {e}", exc_info=True)
                    return False
            except Exception as e:
                logger.error(f"❌ Unexpected error processing quest: {e}", exc_info=True)
                return False

        logger.error("❌ Max retries reached for quest upsert")
        return False
