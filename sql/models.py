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

class AggregatedPokemonIVMonthly(Model):
    """Stores aggregated IV data per spawnpoint, monthly."""
    spawnpoint_id = fields.BigIntField(pk=True)
    latitude = fields.FloatField()
    longitude = fields.FloatField()
    pokemon_id = fields.SmallIntField()
    form = fields.SmallIntField(default=0)
    iv = fields.SmallIntField()
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    total_count = fields.IntField(default=0)

    class Meta:
        table = "aggregated_pokemon_iv_monthly"
        unique_together = (
            "spawnpoint_id", "pokemon_id", "form", "iv", "area", "month_year"
        )

class ShinyUsernameRates(Model):
    """Stores shiny username rates per area."""
    username = fields.CharField(max_length=255, pk=True)
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
