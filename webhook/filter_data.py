from utils.logger import logger
from shapely.geometry import Point, Polygon

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
    def adjust_first_seen_to_local(geofence_name, utc_first_seen:int, offset: int) -> int:
        """
        Adjust a UTC timestamp by a given offset in hours.

        Args:
            utc_first_seen: The original UTC timestamp (in seconds).
            offset: The UTC offset in hours (can be negative, zero, or positive).

        Returns:
            The adjusted timestamp (in seconds) accounting for the offset.
        """
        # Convert the offset to seconds.
        offset_secs = offset * 3600
        # Add the offset to the UTC timestamp.
        adjusted_first_seen = utc_first_seen + offset_secs
        logger.debug(f"🔧 Adjusting timezone for {geofence_name} from UTC: {utc_first_seen} to: {adjusted_first_seen}")
        return adjusted_first_seen

    @staticmethod
    def quest_filter_criteria(message: dict) -> bool:
        """
        Returns True if the message passes basic quest criteria:
        - Contains mandatory fields: "type", "with_ar", "latitude", and "longitude"
        - Contains at least one reward with a valid structure.
        """
        basic_checks = all(key in message for key in ["type", "with_ar", "latitude", "longitude"])
        logger.debug(f"▶️ Quest filter basic checks: {basic_checks} for message: {message.get('type')}")

        rewards = message.get("rewards", [])
        rewards_check = False
        if isinstance(rewards, list) and rewards:
            logger.debug(f"🔍 Found {len(rewards)} rewards in message.")
            for reward in rewards:
                if "type" in reward and "info" in reward:
                    info = reward["info"]
                    # Each reward info must have either (pokemon_id) or (item_id and amount) or (amount)
                    if ("pokemon_id" in info) or (("item_id" in info) and ("amount" in info)) or ("amount" in info):
                        rewards_check = True
                        logger.debug(f"☑️ Reward passed criteria: {reward}")
                    else:
                        logger.warning(f"⚠️ Reward failed criteria (missing required info): {reward}")
                        rewards_check = False
                        break
                else:
                    logger.warning(f"⚠️ Reward missing 'type' or 'info': {reward}")
                    rewards_check = False
                    break
        else:
            logger.debug("❌ No rewards found in message or rewards is not a list.")
        result = basic_checks and rewards_check
        logger.debug(f"✅ Quest filter criteria result: {result}")
        return result

    @staticmethod
    def extract_quest_rewards(quest_rewards: list) -> list:
        """
        Extracts raw reward details from a list of quest rewards.
        Returns a list of reward dictionaries.
        """
        extracted = []
        logger.debug(f"▶️ Extracting rewards from list with {len(quest_rewards)} items.")
        for reward in quest_rewards:
            info = reward.get("info", {})
            reward_data = {
                "reward_type": reward.get("type"),  # Always extract reward type
                "pokemon_id": info.get("pokemon_id"),
                "form_id": info.get("form_id"),
                "item_id": info.get("item_id"),
                "amount": info.get("amount")
            }
            logger.debug(f"☑️ Extracted reward: {reward_data}")
            extracted.append(reward_data)
        logger.debug(f"✅ Extracted {len(extracted)} rewards.")
        return extracted

    @staticmethod
    def process_first_reward(rewards: list, with_ar: bool) -> dict:
        """
        Processes only the first reward in the list and formats the keys.
        Returns a dictionary with processed reward fields.
        """
        processed = {}
        if rewards:
            reward = rewards[0]
            reward_prefix = "reward_ar_" if with_ar else "reward_normal_"
            logger.debug(f"▶️ Processing first reward with prefix '{reward_prefix}': {reward}")
            if reward.get("pokemon_id") is not None:
                processed[f"{reward_prefix}poke_id"] = reward.get("pokemon_id")
                processed[f"{reward_prefix}poke_form"] = reward.get("form_id")
                logger.debug(f"☑️ Set {reward_prefix}poke_id and {reward_prefix}poke_form.")
            if reward.get("item_id") is not None:
                processed[f"{reward_prefix}item_id"] = reward.get("item_id")
                processed[f"{reward_prefix}item_amount"] = reward.get("amount")
                logger.debug(f"☑️ Set {reward_prefix}item_id and {reward_prefix}item_amount.")
            elif reward.get("amount") is not None and reward.get("pokemon_id") is None and reward.get("item_id") is None:
                processed[f"{reward_prefix}item_amount"] = reward.get("amount")
                logger.debug(f"☑️ Set {reward_prefix}item_amount from amount field.")
            # Set the reward type field
            if with_ar:
                processed["reward_ar_type"] = reward.get("reward_type")
                logger.debug(f"☑️ Set reward_ar_type: {processed['reward_ar_type']}.")
            else:
                processed['reward_normal_type'] = reward.get('reward_type')
                logger.debug(f"☑️ Set reward_normal_type: {processed['reward_normal_type']}.")
        else:
            logger.debug("❌ No rewards to process in process_first_reward.")
        return processed

    @staticmethod
    async def calculate_despawn_time(disappear_time, first_seen):
        """Calculate the despawn time for a Pokémon based on its disappear time and first seen time."""
        if disappear_time is None or first_seen is None:
            return None

        time_diff = disappear_time - first_seen
        total_seconds = time_diff // 1
        logger.debug(f"▶️ Despawn timer: {total_seconds}s")
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
                return True, None, None, None  # ✅ Accept data if no geofences exist

            point = Point(longitude, latitude)  # ✅ Create a point with (lon, lat)

            for geofence in self.geofences:
                polygon = Polygon(geofence["coordinates"][0])  # ✅ Convert geofence into Polygon

                if point.within(polygon):  # ✅ Check if point is inside geofence
                    logger.debug(f"✅ Data is inside geofence: {geofence['name']} (ID: {geofence['id']}), offset: {geofence['offset']}")
                    return True, geofence["id"], geofence["name"], geofence["offset"]  # ✅ Return True + Geofence Name

            logger.debug("❌ Data is outside geofenced areas. Ignoring.")
            return False, None, None, None  # ❌ Reject if outside geofences

        except Exception as e:
            logger.error(f"❌ Error checking geofence: {e}")
            return False, None, None, None  # ❌ Reject data if an error occurs

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

        inside_geofence, geofence_id, geofence_name, offset = await self.is_inside_geofence(latitude, longitude)
        if not inside_geofence:
            return None  # ❌ Reject if outside geofence

        # ✅ Handle each webhook type separately
        if data_type == "pokemon":
            pokemon_data = await self.handle_pokemon_data(message, geofence_id, geofence_name, offset)
            if pokemon_data:
                return pokemon_data
        elif data_type == "quest":
            quest_data = await self.handle_quest_data(message, geofence_id, geofence_name, offset)
            if quest_data:
                return quest_data
        elif data_type == "raid":
            raid_data = await self.handle_raid_data(message, geofence_id, geofence_name, offset)
            if raid_data:
                return raid_data
        elif data_type == "invasion":
            invasion_data = await self.handle_invasion_data(message, geofence_id, geofence_name, offset)
            if invasion_data:
                return invasion_data
        else:
            logger.warning(f"⚠️ Unhandled webhook type: {data_type}")
            return None  # ❌ Ignore unknown types

        #except Exception as e:
        #    logger.error(f"❌ Error filtering webhook data: {e}")
        #    return None

    ## ✅ Type-Specific Handling Functions

    async def handle_pokemon_data(self, message, geofence_id, geofence_name, offset: int):
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

        # ✅ Calculate IV percentage
        iv_percentage = self.calculate_iv(
            message["individual_attack"],
            message["individual_defense"],
            message["individual_stamina"]
        )

        # ✅ Extract PVP Ranks
        pvp_ranks = self.extract_pvp_ranks(message.get("pvp", {}))

        # ✅ Adjust first_seen timestamp to local time
        utc_first_seen = int(message["first_seen"])
        corrected_first_seen = self.adjust_first_seen_to_local(geofence_name, utc_first_seen, offset)

        # ✅ Calculate despawn timer
        despawn_timer = await self.calculate_despawn_time(message["disappear_time"], utc_first_seen)

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
            "first_seen": corrected_first_seen,
            "despawn_timer": despawn_timer,
            "weather": message["weather"],
            "spawnpoint": message["spawnpoint_id"],
            "area_id": geofence_id,
            "area_name": geofence_name,
        }

        logger.debug(f"✅ Pokémon {pokemon_data['pokemon_id']} (Form {pokemon_data['form']}) in {geofence_id} - IV: {pokemon_data['iv']}% - Despawns in {despawn_timer} sec")
        return pokemon_data  # ✅ Return structured Pokémon data


    async def handle_quest_data(self, message, geofence_id, geofence_name, offset):
        """
        Process Quest webhook data.
        Returns a dictionary with processed quest data, or None if criteria are not met.
        """
        # Check mandatory fields for quests.
        if not self.quest_filter_criteria(message):
            logger.debug(f"⚠️ Skipping Quest data due to failing criteria: {message}")
            return None

        # ✅ Adjust first_seen timestamp to local time
        utc_first_seen = int(message["updated"])
        corrected_first_seen = self.adjust_first_seen_to_local(geofence_name, utc_first_seen, offset)

        # Build initial quest data structure.
        quest_data = {
            "pokestop_id": message['pokestop_id'],
            "pokestop_name": message['pokestop_name'],
            "latitude": message["latitude"],
            "longitude": message["longitude"],
            "area_name": geofence_name,
            "area_id": geofence_id,
            # Initialize reward fields to None.
            "ar_type": None,
            "normal_type": None,
            "reward_ar_type": None,
            "reward_normal_type": None,
            "reward_ar_item_id": None,
            "reward_ar_item_amount": None,
            "reward_normal_item_id": None,
            "reward_normal_item_amount": None,
            "reward_ar_poke_id": None,
            "reward_ar_poke_form": None,
            "reward_normal_poke_id": None,
            "reward_normal_poke_form": None,
            "first_seen": corrected_first_seen
        }
        # Set quest type based on 'with_ar'
        quest_type_field = 'ar_type' if message.get('with_ar') else 'normal_type'
        quest_data[quest_type_field] = message.get('type')

        # Extract rewards and process only the first reward.
        rewards_extracted = self.extract_quest_rewards(message.get('rewards', []))
        processed_reward = self.process_first_reward(rewards_extracted, message.get('with_ar', False))
        quest_data.update(processed_reward)

        logger.debug(f"✅ Processed quest data: {quest_data}")
        return quest_data


    async def handle_raid_data(self, message, geofence_id, geofence_name, offset):
        """Process Raid webhook data."""
        required_raid_fields = [
            "gym_id",
            "ex_raid_eligible",
            "is_exclusive",
            "level",
            "pokemon_id",
            "form",
            "costume",
            "latitude",
            "longitude"
            ]

        #✅ Check if all required fields are present
        if not all(field in message and message[field] is not None for field in required_raid_fields):
            logger.debug(f"⚠️ Skipping Raid data due to missing fields: {message}")
            return None

        #✅ Extra check: ensure 'pokemon_id' is not None or 0
        pokemon_id_val = message.get("pokemon_id")
        if not pokemon_id_val or int(pokemon_id_val) == 0:
            logger.debug(f"⚠️ Skipping Raid data due to invalid 'pokemon_id': {pokemon_id_val}")
            return None

        # ✅ Adjust first_seen timestamp to local time
        utc_first_seen = int(message["spawn"])
        utc_end = int(message["end"])
        corrected_first_seen = self.adjust_first_seen_to_local(geofence_name, utc_first_seen, offset)
        corrected_end = self.adjust_first_seen_to_local(geofence_name, utc_end, offset)

        # ✅ Extract Raid Data
        raid_data = {
            "raid_pokemon": message["pokemon_id"],
            "raid_level": message["level"],
            "raid_form": message["form"],
            "raid_costume": message["costume"],
            "raid_latitude": message["latitude"],
            "raid_longitude": message["longitude"],
            "raid_is_exclusive": message["is_exclusive"],
            "raid_ex_raid_eligible": message["ex_raid_eligible"],
            "raid_gym_id": message["gym_id"],
            "raid_gym_name": message["gym_name"],
            "raid_team_id": message["team_id"],
            "area_id": geofence_id,
            "area_name": geofence_name,
            "raid_first_seen": corrected_first_seen,
            "raid_end": corrected_end,
        }

        logger.debug(f"✅ Raid {raid_data['raid_level']} - Boss {raid_data['raid_pokemon']} in Area: {raid_data['area_name']} with Spawn timer: {raid_data['raid_first_seen']}")
        return raid_data

    async def handle_invasion_data(self, message, geofence_id, geofence_name, offset):
        """Process Invasion (Rocket) webhook data."""
        required_invasion_fields = [
            "display_type",
            "character",
            "confirmed",
            "pokestop_id",
            "start",
            "latitude",
            "longitude"
        ]
        # ✅ Check if all required fields are present
        if not all(field in message and message[field] is not None for field in required_invasion_fields):
            logger.debug(f"⚠️ Skipping Invasion data due to missing fields: {message}")
            return None

        # ✅ Adjust first_seen timestamp to local time
        utc_first_seen = int(message["start"])
        corrected_first_seen = self.adjust_first_seen_to_local(geofence_name, utc_first_seen, offset)

        # ✅ Extract Invasion Data
        invasion_data = {
            "invasion_type": message["display_type"],
            "invasion_character": message["character"],
            "invasion_grunt_type": message["grunt_type"],
            "invasion_confirmed": message["confirmed"],
            "invasion_pokestop_id": message["pokestop_id"],
            "invasion_pokestop_name": message["pokestop_name"],
            "invasion_latitude": message["latitude"],
            "invasion_longitude": message["longitude"],
            "invasion_first_seen": corrected_first_seen,
            "area_id": geofence_id,
            "area_name": geofence_name,
        }

        logger.debug(f"✅ Invasion Type: {invasion_data['invasion_type']}, Character: {invasion_data['invasion_character']} in Area {invasion_data['area_name']}. First seen at: {invasion_data['invasion_first_seen']}")
        return invasion_data
