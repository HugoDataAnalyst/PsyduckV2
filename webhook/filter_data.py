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

    # Helper functions
    @staticmethod
    async def calculate_despawn_time(disappear_time, first_seen):
        """Calculate the despawn time for a Pokémon based on its disappear time and first seen time."""
        if disappear_time is None or first_seen is None:
            return None

        time_diff = disappear_time - first_seen
        total_seconds = time_diff // 1
        return total_seconds

    @staticmethod
    def calculate_iv(attack, defense, stamina):
        """Calculate Pokémon IV percentage and round to 2 decimal places."""
        iv = round(((attack + defense + stamina) / 45) * 100, 2)
        return iv

    @staticmethod
    def extract_pvp_ranks(pvp_data):
        """Extract top 1 PVP rankings for Great, Little, and Ultra leagues."""
        ranks = {f'pvp_{category}_rank': None for category in ['great', 'little', 'ultra']}

        if pvp_data:
            for category in list(ranks.keys()):
                category_data = pvp_data.get(category, [])
                top_ranks = sorted([entry.get('rank') for entry in category_data if entry.get('rank') is not None])
                # Store only if in Top 1
                ranks[f'pvp_{category}_rank'] = [rank for rank in top_ranks if rank == 1] or None

        return ranks

    async def is_inside_geofence(self, latitude, longitude):
        """Check if given coordinates are inside the latest cached geofences
            Returns a tuple (inside: bool, geofence_id: int or None)
        """
        try:
            if not self.geofences:
                logger.warning("⚠️ No geofences available. Accepting all data by default.")
                return True, None, None  # ✅ Accept data if no geofences exist

            point = Point(longitude, latitude)  # ✅ Create a point with (lon, lat)

            for geofence in self.geofences:
                polygon = Polygon(geofence["coordinates"][0])  # ✅ Convert geofence into Polygon

                if point.within(polygon):  # ✅ Check if point is inside geofence
                    logger.debug(f"✅ Data is inside geofence: {geofence['name']} (ID: {geofence['id']})")
                    return True, geofence["id"], geofence["name"]  # ✅ Return True + Geofence Name

            logger.debug("❌ Data is outside geofenced areas. Ignoring.")
            return False, None, None  # ❌ Reject if outside geofences

        except Exception as e:
            logger.error(f"❌ Error checking geofence: {e}")
            return False, None, None  # ❌ Reject data if an error occurs

    async def filter_webhook_data(self, data):
        """Filter webhook data based on type and geofence validation."""
        #try:
        data_type = data.get("type")
        if data_type not in self.allowed_types:
            logger.debug(f"❌ Ignoring webhook type: {data_type}")
            return None

        message = data.get("message", {})
        latitude, longitude = message.get("latitude"), message.get("longitude")

        if latitude is None or longitude is None:
            logger.debug("⚠️ Webhook data missing coordinates. Ignoring.")
            return None

        inside_geofence, geofence_id, geofence_name = await self.is_inside_geofence(latitude, longitude)
        if not inside_geofence:
            return None  # ❌ Reject if outside geofence

        # ✅ Handle each webhook type separately
        if data_type == "pokemon":
            pokemon_data = await self.handle_pokemon_data(message, geofence_id, geofence_name)
            if pokemon_data:
                return pokemon_data
        #elif data_type == "quest":
        #    return self.handle_quest_data(message, geofence_id)
        #elif data_type == "raid":
        #    return self.handle_raid_data(message, geofence_id)
        #elif data_type == "invasion":
        #    return self.handle_invasion_data(message, geofence_id)
        else:
            logger.warning(f"⚠️ Unhandled webhook type: {data_type}")
            return None  # ❌ Ignore unknown types

        #except Exception as e:
        #    logger.error(f"❌ Error filtering webhook data: {e}")
        #    return None

    ## ✅ Type-Specific Handling Functions

    async def handle_pokemon_data(self, message, geofence_id, geofence_name):
        """Process and filter Pokémon webhook data."""
        required_fields = [
            "pokemon_id",
            "latitude",
            "longitude",
            "individual_attack",
            "individual_defense",
            "individual_stamina",
            "disappear_time",
            "first_seen",
            "spawnpoint_id"
        ]

        # ✅ Check if all required fields are present
        if not all(field in message and message[field] is not None for field in required_fields):
            logger.debug(f"⚠️ Skipping Pokémon data due to missing fields: {message}")
            return None

        # ✅ Calculate despawn timer
        despawn_timer = await self.calculate_despawn_time(message["disappear_time"], message["first_seen"])

        # ✅ Calculate IV percentage
        iv_percentage = self.calculate_iv(
            message["individual_attack"],
            message["individual_defense"],
            message["individual_stamina"]
        )

        # ✅ Extract PVP Ranks
        pvp_ranks = self.extract_pvp_ranks(message.get("pvp", {}))

        # ✅ Extract Pokémon Data
        pokemon_data = {
            "pokemon_id": message["pokemon_id"],
            "form": message["form"],
            "latitude": message["latitude"],
            "longitude": message["longitude"],
            **pvp_ranks,
            "iv": iv_percentage,
            "cp": message["cp"],
            "level": message["pokemon_level"],
            "gender": message["gender"],
            "shiny": message["shiny"],
            "size": message["size"],
            "username": message["username"],
            "first_seen": message["first_seen"],
            "despawn_timer": despawn_timer,
            "spawnpoint": message["spawnpoint_id"],
            "area_id": geofence_id,
            "area_name": geofence_name,
        }

        logger.debug(f"✅ Pokémon {pokemon_data['pokemon_id']} (Form {pokemon_data['form']}) in {geofence_id} - IV: {pokemon_data['iv']}% - Despawns in {despawn_timer} sec")
        return pokemon_data  # ✅ Return structured Pokémon data


    def handle_quest_data(self, message, geofence_id):
        """Process Quest webhook data."""
        quest_title = message.get("quest_title")
        reward = message.get("quest_reward")

        logger.info(f"✅ Quest '{quest_title}' in {geofence_id} - Reward: {reward}")
        return {
            "type": "quest",
            "quest_title": quest_title,
            "reward": reward,
            "geofence": geofence_id,
        }

    def handle_raid_data(self, message, geofence_id):
        """Process Raid webhook data."""
        boss_pokemon = message.get("raid_pokemon_id")
        level = message.get("raid_level")

        logger.info(f"✅ Raid {level} - Boss {boss_pokemon} in {geofence_id}")
        return {
            "type": "raid",
            "raid_pokemon": boss_pokemon,
            "level": level,
            "geofence": geofence_id,
        }

    def handle_invasion_data(self, message, geofence_id):
        """Process Invasion (Rocket) webhook data."""
        grunt_type = message.get("grunt_type")

        logger.info(f"✅ Rocket Grunt '{grunt_type}' in {geofence_id}")
        return {
            "type": "invasion",
            "grunt_type": grunt_type,
            "geofence": geofence_id,
        }
