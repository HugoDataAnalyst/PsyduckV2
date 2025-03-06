from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS `spawnpoint` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `latitude` DOUBLE NOT NULL,
    `longitude` DOUBLE NOT NULL,
    `inserted_at` INT NOT NULL
) CHARACTER SET utf8mb4 COMMENT='Stores unique spawnpoint locations to reduce redundant lat/lon storage.';
CREATE TABLE IF NOT EXISTS `pokemonsighting` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `pokemon_id` INT NOT NULL,
    `form` VARCHAR(30),
    `latitude` DOUBLE NOT NULL,
    `longitude` DOUBLE NOT NULL,
    `iv` INT NOT NULL,
    `username` VARCHAR(50) NOT NULL,
    `pvp` LONGTEXT,
    `seen_at` INT NOT NULL,
    `expire_timestamp` INT NOT NULL,
    `spawnpoint_id` BIGINT,
    CONSTRAINT `fk_pokemons_spawnpoi_a80341a7` FOREIGN KEY (`spawnpoint_id`) REFERENCES `spawnpoint` (`id`) ON DELETE SET NULL
) CHARACTER SET utf8mb4 COMMENT='Stores Pokémon sightings, referencing spawnpoints when available.';
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
