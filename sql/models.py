from tortoise.models import Model
from tortoise import fields
import config as AppConfig

class AreaNames(Model):
    """Stores area names and their associated numeric IDs."""
    id = fields.SmallIntField(pk=True)
    name = fields.CharField(max_length=255, unique=True)

    class Meta:
        table = "area_names"

class Spawnpoint(Model):
    """Stores spawnpoint information."""
    id = fields.BigIntField(pk=True)
    spawnpoint = fields.BigIntField(unique=True)
    latitude = fields.FloatField()
    longitude = fields.FloatField()

    class Meta:
        table = "spawnpoints"

class Pokestops(Model):
    """Stores pokestop information"""
    id = fields.BigIntField(pk=True)
    pokestop = fields.CharField(max_length=50, unique=True)
    pokestop_name = fields.CharField(max_length=255)
    latitude = fields.FloatField()
    longitude = fields.FloatField()

    class Meta:
        table = "pokestops"

class Gyms(Model):
    """Stores gym information."""
    id = fields.BigIntField(pk=True)
    gym = fields.CharField(max_length=50, unique=True)
    gym_name = fields.CharField(max_length=255)
    latitude = fields.FloatField()
    longitude = fields.FloatField()

    class Meta:
        table = "gyms"

class AggregatedQuests(Model):
    """Stores aggregated quest data per pokestop, monthly"""
    id = fields.BigIntField(pk=True)
    pokestop = fields.ForeignKeyField("models.Pokestops", related_name="aggregated_quests_stats")
    ar_type = fields.SmallIntField()
    normal_type = fields.SmallIntField()
    reward_ar_type = fields.SmallIntField()
    reward_normal_type = fields.SmallIntField()
    reward_ar_item_id = fields.SmallIntField()
    reward_ar_item_amount = fields.SmallIntField()
    reward_normal_item_id = fields.SmallIntField()
    reward_normal_item_amount = fields.SmallIntField()
    reward_ar_poke_id = fields.SmallIntField()
    reward_ar_poke_form = fields.CharField(max_length=15)
    reward_normal_poke_id = fields.SmallIntField()
    reward_normal_poke_form = fields.CharField(max_length=15)
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_quests_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "aggregated_quests"
        unique_together = (
            "pokestop", "ar_type", "normal_type",
            "reward_ar_type", "reward_normal_type",
            "reward_ar_item_id", "reward_ar_item_amount", "reward_normal_item_id",
            "reward_normal_item_amount", "reward_ar_poke_id", "reward_ar_poke_form",
            "reward_normal_poke_id", "reward_normal_poke_form", "area", "month_year",
        )

class AggreagatedInvasions(Model):
    """Stores aggregated invasion data per gym, monthly."""
    id = fields.BigIntField(pk=True)
    pokestop = fields.ForeignKeyField("models.Pokestops", related_name="aggregated_invasions_stats")
    display_type = fields.SmallIntField()
    character = fields.SmallIntField()
    grunt = fields.SmallIntField()
    confirmed = fields.SmallIntField()
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_invasions_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "aggregated_invasions"
        unique_together = (
            "pokestop", "display_type", "character",
            "grunt", "confirmed", "area", "month_year"
        )

class AggregatedRaids(Model):
    """Stores aggregated raid data per gym, monthly."""
    id = fields.BigIntField(pk=True)
    gym = fields.ForeignKeyField("models.Gyms", related_name="aggregated_raids_stats")
    raid_pokemon = fields.SmallIntField()
    raid_level = fields.SmallIntField()
    raid_form = fields.CharField(max_length=15)
    raid_team = fields.SmallIntField(default=0)
    raid_costume = fields.SmallIntField(default=0)
    raid_is_exclusive = fields.SmallIntField(default=0)
    raid_ex_raid_eligible = fields.SmallIntField(default=0)
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_raids_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "aggregated_raids"
        unique_together = (
            "gym", "raid_pokemon", "raid_level",
            "raid_form", "raid_team", "raid_costume",
            "raid_is_exclusive", "raid_ex_raid_eligible",
            "area", "month_year"
        )

class AggregatedPokemonIVMonthly(Model):
    """Stores aggregated IV data per spawnpoint, monthly."""
    id = fields.BigIntField(pk=True)
    spawnpoint = fields.ForeignKeyField("models.Spawnpoint", related_name="aggregated_stats")
    pokemon_id = fields.SmallIntField()
    form = fields.CharField(max_length=15)
    iv = fields.SmallIntField()
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "aggregated_pokemon_iv_monthly"
        unique_together = (
            "spawnpoint", "pokemon_id", "form",
            "iv", "area", "month_year"
        )

class ShinyUsernameRates(Model):
    """Stores shiny username rates per area."""
    id = fields.BigIntField(pk=True)
    username = fields.CharField(max_length=255)
    pokemon_id = fields.SmallIntField()
    form = fields.CharField(max_length=15)
    shiny = fields.SmallIntField(default=0)  # 0 = Not Shiny, 1 = Shiny
    area = fields.ForeignKeyField("models.AreaNames", related_name="shiny_username_rates")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "shiny_username_rates"
        unique_together = (
            "username", "pokemon_id", "form",
            "shiny", "area", "month_year"
        )

# Optional: Register models for Tortoise
TORTOISE_ORM = {
    "connections": {
        "default": f"mysql://{AppConfig.db_user}:{AppConfig.db_password}@{AppConfig.db_host}:{AppConfig.db_port}/{AppConfig.db_name}"
    },
    "apps": {
        "models": {
            "models": ["sql.models", "aerich.models"],
            "default_connection": "default",
        }
    }
}
