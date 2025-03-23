from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE `aggregated_pokemon_iv_monthly` DROP FOREIGN KEY `fk_aggregat_spawnpoi_d55caa81`;
        ALTER TABLE `aggregated_pokemon_iv_monthly` RENAME COLUMN `spawnpoint_id` TO `spawnpoint`;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE `aggregated_pokemon_iv_monthly` RENAME COLUMN `spawnpoint` TO `spawnpoint_id`;
        ALTER TABLE `aggregated_pokemon_iv_monthly` ADD CONSTRAINT `fk_aggregat_spawnpoi_d55caa81` FOREIGN KEY (`spawnpoint_id`) REFERENCES `spawnpoints` (`id`) ON DELETE CASCADE;"""
