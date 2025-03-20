from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE `aggregated_raids` ALTER COLUMN `raid_costume` DROP DEFAULT;
        ALTER TABLE `aggregated_raids` MODIFY COLUMN `raid_costume` VARCHAR(15) NOT NULL;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE `aggregated_raids` MODIFY COLUMN `raid_costume` SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE `aggregated_raids` ALTER COLUMN `raid_costume` SET DEFAULT 0;"""
