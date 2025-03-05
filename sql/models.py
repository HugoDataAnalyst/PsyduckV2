from tortoise.models import Model
from tortoise import fields
import config as AppConfig

class Spawnpoint(Model):
    """Stores unique spawnpoint locations to reduce redundant lat/lon storage."""
    id = fields.BigIntField(pk=True)  # Unique spawnpoint ID from the webhook
    latitude = fields.FloatField()
    longitude = fields.FloatField()
    inserted_at = fields.IntField()  # Unix timestamp


class PokemonSighting(Model):
    """Stores Pokémon sightings, referencing spawnpoints when available."""
    id = fields.IntField(pk=True)
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=30, null=True)
    latitude = fields.FloatField()  # Stored if spawnpoint_id is NULL
    longitude = fields.FloatField()  # Stored if spawnpoint_id is NULL
    iv = fields.IntField()
    username = fields.CharField(max_length=50)
    pvp = fields.TextField(null=True)  # Stores rankings for leagues
    seen_at = fields.IntField()  # Unix timestamp
    expire_timestamp = fields.IntField()  # Unix timestamp
    spawnpoint = fields.ForeignKeyField(
        "models.Spawnpoint",
        null=True,
        on_delete=fields.SET_NULL
    )  # Optional foreign key to spawnpoints


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
            "models": ["sql.models"],
            "default_connection": "default",
        }
    }
}
