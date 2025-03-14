from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS `pokestops` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `pokestop` VARCHAR(50) NOT NULL UNIQUE,
    `pokestop_name` VARCHAR(255) NOT NULL,
    `latitude` DOUBLE NOT NULL,
    `longitude` DOUBLE NOT NULL
) CHARACTER SET utf8mb4 COMMENT='Stores pokestop information';
        CREATE TABLE IF NOT EXISTS `gyms` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `gym` VARCHAR(50) NOT NULL UNIQUE,
    `gym_name` VARCHAR(255) NOT NULL,
    `latitude` DOUBLE NOT NULL,
    `longitude` DOUBLE NOT NULL
) CHARACTER SET utf8mb4 COMMENT='Stores gym information.';
        CREATE TABLE IF NOT EXISTS `aggregated_invasions` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `display_type` SMALLINT NOT NULL,
    `character` SMALLINT NOT NULL,
    `grunt` SMALLINT NOT NULL,
    `confirmed` SMALLINT NOT NULL,
    `month_year` SMALLINT NOT NULL,
    `total_count` INT NOT NULL DEFAULT 0,
    `area_id` SMALLINT NOT NULL,
    `pokestop_id` BIGINT NOT NULL,
    UNIQUE KEY `uid_aggregated__pokesto_4a632d` (`pokestop_id`, `display_type`, `character`, `grunt`, `confirmed`, `area_id`, `month_year`),
    CONSTRAINT `fk_aggregat_area_nam_1bc14a56` FOREIGN KEY (`area_id`) REFERENCES `area_names` (`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_aggregat_pokestop_1c193e9b` FOREIGN KEY (`pokestop_id`) REFERENCES `pokestops` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='Stores aggregated invasion data per gym, monthly.';
        ALTER TABLE `aggregated_pokemon_iv_monthly` ALTER COLUMN `form` DROP DEFAULT;
        ALTER TABLE `aggregated_pokemon_iv_monthly` MODIFY COLUMN `form` VARCHAR(15) NOT NULL;
        CREATE TABLE IF NOT EXISTS `aggregated_quests` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `ar_type` SMALLINT NOT NULL,
    `normal_type` SMALLINT NOT NULL,
    `reward_ar_type` SMALLINT NOT NULL,
    `reward_normal_type` SMALLINT NOT NULL,
    `reward_ar_item_id` SMALLINT NOT NULL,
    `reward_ar_item_amount` SMALLINT NOT NULL,
    `reward_normal_item_id` SMALLINT NOT NULL,
    `reward_normal_item_amount` SMALLINT NOT NULL,
    `reward_ar_poke_id` SMALLINT NOT NULL,
    `reward_ar_poke_form` VARCHAR(15) NOT NULL,
    `reward_normal_poke_id` SMALLINT NOT NULL,
    `reward_normal_poke_form` VARCHAR(15) NOT NULL,
    `month_year` SMALLINT NOT NULL,
    `total_count` INT NOT NULL DEFAULT 0,
    `area_id` SMALLINT NOT NULL,
    `pokestop_id` BIGINT NOT NULL,
    UNIQUE KEY `uid_aggregated__pokesto_24c120` (`pokestop_id`, `ar_type`, `normal_type`, `reward_ar_type`, `reward_normal_type`, `reward_ar_item_id`, `reward_ar_item_amount`, `reward_normal_item_id`, `reward_normal_item_amount`, `reward_ar_poke_id`, `reward_ar_poke_form`, `reward_normal_poke_id`, `reward_normal_poke_form`, `area_id`, `month_year`),
    CONSTRAINT `fk_aggregat_area_nam_cb22b185` FOREIGN KEY (`area_id`) REFERENCES `area_names` (`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_aggregat_pokestop_3a70f071` FOREIGN KEY (`pokestop_id`) REFERENCES `pokestops` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='Stores aggregated quest data per pokestop, monthly';
        CREATE TABLE IF NOT EXISTS `aggregated_raids` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `raid_pokemon` SMALLINT NOT NULL,
    `raid_level` SMALLINT NOT NULL,
    `raid_form` VARCHAR(15) NOT NULL,
    `raid_team` SMALLINT NOT NULL DEFAULT 0,
    `raid_costume` SMALLINT NOT NULL DEFAULT 0,
    `raid_is_exclusive` SMALLINT NOT NULL DEFAULT 0,
    `raid_ex_raid_eligible` SMALLINT NOT NULL DEFAULT 0,
    `month_year` SMALLINT NOT NULL,
    `total_count` INT NOT NULL DEFAULT 0,
    `area_id` SMALLINT NOT NULL,
    `gym_id` BIGINT NOT NULL,
    UNIQUE KEY `uid_aggregated__gym_id_1c004b` (`gym_id`, `raid_pokemon`, `raid_level`, `raid_form`, `raid_team`, `raid_costume`, `raid_is_exclusive`, `raid_ex_raid_eligible`, `area_id`, `month_year`),
    CONSTRAINT `fk_aggregat_area_nam_559f8d01` FOREIGN KEY (`area_id`) REFERENCES `area_names` (`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_aggregat_gyms_36f1f564` FOREIGN KEY (`gym_id`) REFERENCES `gyms` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='Stores aggregated raid data per gym, monthly.';
        ALTER TABLE `shiny_username_rates` ALTER COLUMN `form` DROP DEFAULT;
        ALTER TABLE `shiny_username_rates` MODIFY COLUMN `form` VARCHAR(15) NOT NULL;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE `shiny_username_rates` MODIFY COLUMN `form` SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE `shiny_username_rates` ALTER COLUMN `form` SET DEFAULT 0;
        ALTER TABLE `aggregated_pokemon_iv_monthly` MODIFY COLUMN `form` SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE `aggregated_pokemon_iv_monthly` ALTER COLUMN `form` SET DEFAULT 0;
        DROP TABLE IF EXISTS `gyms`;
        DROP TABLE IF EXISTS `aggregated_invasions`;
        DROP TABLE IF EXISTS `aggregated_raids`;
        DROP TABLE IF EXISTS `aggregated_quests`;
        DROP TABLE IF EXISTS `pokestops`;"""
