from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS `area_names` (
    `id` SMALLINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL UNIQUE
) CHARACTER SET utf8mb4 COMMENT='Stores area names and their associated numeric IDs.';
CREATE TABLE IF NOT EXISTS `shiny_username_rates` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `username` VARCHAR(255) NOT NULL,
    `pokemon_id` SMALLINT NOT NULL,
    `form` SMALLINT NOT NULL DEFAULT 0,
    `shiny` SMALLINT NOT NULL DEFAULT 0,
    `month_year` SMALLINT NOT NULL,
    `total_count` INT NOT NULL DEFAULT 0,
    `area_id` SMALLINT NOT NULL,
    UNIQUE KEY `uid_shiny_usern_usernam_7404f9` (`username`, `pokemon_id`, `form`, `shiny`, `area_id`, `month_year`),
    CONSTRAINT `fk_shiny_us_area_nam_2c39d5e1` FOREIGN KEY (`area_id`) REFERENCES `area_names` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='Stores shiny username rates per area.';
CREATE TABLE IF NOT EXISTS `spawnpoints` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `spawnpoint` BIGINT NOT NULL UNIQUE,
    `latitude` DOUBLE NOT NULL,
    `longitude` DOUBLE NOT NULL
) CHARACTER SET utf8mb4 COMMENT='Stores spawnpoint information.';
CREATE TABLE IF NOT EXISTS `aggregated_pokemon_iv_monthly` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `pokemon_id` SMALLINT NOT NULL,
    `form` SMALLINT NOT NULL DEFAULT 0,
    `iv` SMALLINT NOT NULL,
    `month_year` SMALLINT NOT NULL,
    `total_count` INT NOT NULL DEFAULT 0,
    `area_id` SMALLINT NOT NULL,
    `spawnpoint_id` BIGINT NOT NULL,
    UNIQUE KEY `uid_aggregated__spawnpo_1ff10c` (`spawnpoint_id`, `pokemon_id`, `form`, `iv`, `area_id`, `month_year`),
    CONSTRAINT `fk_aggregat_area_nam_33d0e133` FOREIGN KEY (`area_id`) REFERENCES `area_names` (`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_aggregat_spawnpoi_d55caa81` FOREIGN KEY (`spawnpoint_id`) REFERENCES `spawnpoints` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='Stores aggregated IV data per spawnpoint, monthly.';
CREATE TABLE IF NOT EXISTS `aerich` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `version` VARCHAR(255) NOT NULL,
    `app` VARCHAR(100) NOT NULL,
    `content` JSON NOT NULL
) CHARACTER SET utf8mb4;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        """
