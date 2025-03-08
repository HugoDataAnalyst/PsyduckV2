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
    spawnpoint_id = fields.BigIntField()
    latitude = fields.FloatField()
    longitude = fields.FloatField()
    pokemon_id = fields.SmallIntField()
    form = fields.SmallIntField(default=0)
    iv = fields.SmallIntField()
    level = fields.SmallIntField()
    gender = fields.SmallIntField()
    size = fields.SmallIntField()
    area = fields.ForeignKeyField("models.AreaNames", related_name="aggregated_stats")
    month_year = fields.SmallIntField()  # Format: YYMM (2503 for March 2025)
    shiny = fields.SmallIntField(default=0)  # 0 = Not Shiny, 1 = Shiny
    total_count = fields.IntField(default=1)
    pvp_little_rank = fields.BooleanField(null=True)
    pvp_great_rank = fields.BooleanField(null=True)
    pvp_ultra_rank = fields.BooleanField(null=True)

    class Meta:
        table = "aggregated_pokemon_iv_monthly"
        unique_together = (
            "spawnpoint_id", "pokemon_id", "form", "iv", "level", "gender", "size", "shiny", "area", "month_year"
        )


class TotalPokemonStats(Model):
    """Tracks cumulative Pokémon counts per area."""
    area_name = fields.CharField(max_length=255, pk=True)  # Primary key (area-based)
    total = fields.BigIntField(default=0)  # Total Pokémon spotted
    total_iv100 = fields.BigIntField(default=0)  # Total IV 100 Pokémon
    total_iv0 = fields.BigIntField(default=0)  # Total IV 0 Pokémon
    total_top_1_little = fields.BigIntField(default=0)  # Top 1 Little League
    total_top_1_great = fields.BigIntField(default=0)  # Top 1 Great League
    total_top_1_ultra = fields.BigIntField(default=0)  # Top 1 Ultra League
    total_shiny = fields.BigIntField(default=0)  # Total shiny Pokémon


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
