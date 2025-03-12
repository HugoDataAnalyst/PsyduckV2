from calendar import month
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

class AggregatedPokemonIVMonthly(Model):
    """Stores aggregated IV data per spawnpoint, monthly."""
    id = fields.BigIntField(pk=True)
    spawnpoint = fields.ForeignKeyField("models.Spawnpoint", related_name="aggregated_stats")
    pokemon_id = fields.SmallIntField()
    form = fields.SmallIntField(default=0)
    iv = fields.SmallIntField()
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "aggregated_pokemon_iv_monthly"
        unique_together = (
            "spawnpoint", "pokemon_id", "form", "iv", "area", "month_year"
        )

class ShinyUsernameRates(Model):
    """Stores shiny username rates per area."""
    id = fields.BigIntField(pk=True)
    username = fields.CharField(max_length=255)
    pokemon_id = fields.SmallIntField()
    form = fields.SmallIntField(default=0)
    shiny = fields.SmallIntField(default=0)  # 0 = Not Shiny, 1 = Shiny
    area = fields.ForeignKeyField("models.AreaNames", related_name="shiny_username_rates")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "shiny_username_rates"
        unique_together = (
            "username", "pokemon_id", "form", "shiny", "area", "month_year"
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
