from shapely.geometry import Point, Polygon
from utils.logger import logger

class WebhookFilter:
    """Filters incoming webhook data based on type and geofence location."""

    def __init__(self, allowed_types, geofences):
        """
        Initialize webhook filter with specific types and latest geofences.

        :param allowed_types: Set of allowed webhook types (e.g., {"pokemon", "raid"}).
        :param geofences: Cached geofences from FastAPI startup.
        """
        self.allowed_types = allowed_types
        self.geofences = geofences  # ✅ Inject geofences dynamically

    async def is_inside_geofence(self, latitude, longitude):
        """Check if given coordinates are inside the latest cached geofences."""
        try:
            if not self.geofences:
                logger.warning("⚠️ No geofences available. Accepting all data by default.")
                return True, None  # ✅ Accept data if no geofences exist

            point = Point(longitude, latitude)  # ✅ Create a point with (lon, lat)

            for geofence in self.geofences:
                polygon = Polygon(geofence["coordinates"][0])  # ✅ Convert geofence into Polygon

                if point.within(polygon):  # ✅ Check if point is inside geofence
                    logger.debug(f"✅ Data is inside geofence: {geofence['name']}")
                    return True, geofence["name"]  # ✅ Return True + Geofence Name

            logger.debug("❌ Data is outside geofenced areas. Ignoring.")
            return False, None  # ❌ Reject if outside geofences

        except Exception as e:
            logger.error(f"❌ Error checking geofence: {e}")
            return False, None  # ❌ Reject data if an error occurs

    async def filter_webhook_data(self, data):
        """Filter webhook data based on type and geofence validation."""
        try:
            data_type = data.get("type")
            if data_type not in self.allowed_types:
                logger.debug(f"❌ Ignoring webhook type: {data_type}")
                return None

            message = data.get("message", {})
            latitude, longitude = message.get("latitude"), message.get("longitude")

            if latitude is None or longitude is None:
                logger.debug("⚠️ Webhook data missing coordinates. Ignoring.")
                return None

            inside_geofence, geofence_name = await self.is_inside_geofence(latitude, longitude)
            if not inside_geofence:
                return None  # ❌ Reject if outside geofence

            # ✅ Handle each webhook type separately
            if data_type == "pokemon":
                return self.handle_pokemon_data(message, geofence_name)
            elif data_type == "quest":
                return self.handle_quest_data(message, geofence_name)
            elif data_type == "raid":
                return self.handle_raid_data(message, geofence_name)
            elif data_type == "invasion":
                return self.handle_invasion_data(message, geofence_name)

            logger.warning(f"⚠️ Unhandled webhook type: {data_type}")
            return None  # ❌ Ignore unknown types

        except Exception as e:
            logger.error(f"❌ Error filtering webhook data: {e}")
            return None

    ## ✅ Type-Specific Handling Functions

    def handle_pokemon_data(self, message, geofence_name):
        """Process Pokémon webhook data."""
        pokemon_id = message.get("pokemon_id")
        form = message.get("form", None)
        cp = message.get("cp")
        iv = message.get("individual_attack"), message.get("individual_defense"), message.get("individual_stamina")

        logger.info(f"✅ Pokémon {pokemon_id} (Form {form}) in {geofence_name} - CP: {cp}, IV: {iv}")
        return {
            "type": "pokemon",
            "pokemon_id": pokemon_id,
            "form": form,
            "cp": cp,
            "iv": iv,
            "geofence": geofence_name,
        }

    def handle_quest_data(self, message, geofence_name):
        """Process Quest webhook data."""
        quest_title = message.get("quest_title")
        reward = message.get("quest_reward")

        logger.info(f"✅ Quest '{quest_title}' in {geofence_name} - Reward: {reward}")
        return {
            "type": "quest",
            "quest_title": quest_title,
            "reward": reward,
            "geofence": geofence_name,
        }

    def handle_raid_data(self, message, geofence_name):
        """Process Raid webhook data."""
        boss_pokemon = message.get("raid_pokemon_id")
        level = message.get("raid_level")

        logger.info(f"✅ Raid {level} - Boss {boss_pokemon} in {geofence_name}")
        return {
            "type": "raid",
            "raid_pokemon": boss_pokemon,
            "level": level,
            "geofence": geofence_name,
        }

    def handle_invasion_data(self, message, geofence_name):
        """Process Invasion (Rocket) webhook data."""
        grunt_type = message.get("grunt_type")

        logger.info(f"✅ Rocket Grunt '{grunt_type}' in {geofence_name}")
        return {
            "type": "invasion",
            "grunt_type": grunt_type,
            "geofence": geofence_name,
        }
