from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS `area_names` (
    `id` SMALLINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL UNIQUE
) CHARACTER SET utf8mb4 COMMENT='Stores area names and their associated numeric IDs.';
CREATE TABLE IF NOT EXISTS `aggregated_pokemon_iv_monthly` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `spawnpoint_id` BIGINT NOT NULL,
    `latitude` DOUBLE NOT NULL,
    `longitude` DOUBLE NOT NULL,
    `pokemon_id` SMALLINT NOT NULL,
    `form` SMALLINT NOT NULL DEFAULT 0,
    `iv` SMALLINT NOT NULL,
    `level` SMALLINT NOT NULL,
    `gender` SMALLINT NOT NULL,
    `size` SMALLINT NOT NULL,
    `month_year` SMALLINT NOT NULL,
    `shiny` SMALLINT NOT NULL DEFAULT 0,
    `total_count` INT NOT NULL DEFAULT 1,
    `pvp_little_rank` BOOL,
    `pvp_great_rank` BOOL,
    `pvp_ultra_rank` BOOL,
    `area_id` SMALLINT NOT NULL,
    UNIQUE KEY `uid_aggregated__spawnpo_95c765` (`spawnpoint_id`, `pokemon_id`, `form`, `iv`, `level`, `gender`, `size`, `shiny`, `area_id`, `month_year`),
    CONSTRAINT `fk_aggregat_area_nam_33d0e133` FOREIGN KEY (`area_id`) REFERENCES `area_names` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='Stores aggregated IV data per spawnpoint, monthly.';
CREATE TABLE IF NOT EXISTS `totalpokemonstats` (
    `area_name` VARCHAR(255) NOT NULL PRIMARY KEY,
    `total` BIGINT NOT NULL DEFAULT 0,
    `total_iv100` BIGINT NOT NULL DEFAULT 0,
    `total_iv0` BIGINT NOT NULL DEFAULT 0,
    `total_top_1_little` BIGINT NOT NULL DEFAULT 0,
    `total_top_1_great` BIGINT NOT NULL DEFAULT 0,
    `total_top_1_ultra` BIGINT NOT NULL DEFAULT 0,
    `total_shiny` BIGINT NOT NULL DEFAULT 0
) CHARACTER SET utf8mb4 COMMENT='Tracks cumulative Pokémon counts per area.';
CREATE TABLE IF NOT EXISTS `aerich` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `version` VARCHAR(255) NOT NULL,
    `app` VARCHAR(100) NOT NULL,
    `content` JSON NOT NULL
) CHARACTER SET utf8mb4;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        """
