from datetime import datetime
from utils.logger import logger
from sql import models
from tortoise.expressions import Q, F

class RaidSQLQueries():
    def __init__(self, area: str, start: datetime, end: datetime,
                 gym_id: str = "all", raid_pokemon: str = "all",
                 raid_level: str = "all", raid_form: str = "all",
                 raid_team: str = "all", raid_costume: str = "all",
                 raid_is_exclusive: str = "all",
                 raid_ex_raid_eligible: str = "all",
                 limit: int = 0):
        self.area = area
        self.start = start
        self.end = end
        self.gym_id = gym_id
        self.raid_pokemon = raid_pokemon
        self.raid_level = raid_level
        self.raid_form = raid_form
        self.raid_team = raid_team
        self.raid_costume = raid_costume
        self.raid_is_exclusive = raid_is_exclusive
        self.raid_ex_raid_eligible = raid_ex_raid_eligible
        self.limit = limit

    async def raid_sql_data(self) -> dict:
        filters = Q(month_year__gte=self.start) & Q(month_year__lte=self.end)

        if self.area.lower() != "global":
            filters &= Q(area__name=self.area)

        if self.gym_id != "all":
            filters &= Q(gym__gym=self.gym_id)

        if self.raid_pokemon != "all":
            filters &= Q(raid_pokemon=int(self.raid_pokemon))

        if self.raid_level != "all":
            filters &= Q(raid_level=int(self.raid_level))

        if self.raid_form != "all":
            filters &= Q(raid_form=self.raid_form)

        if self.raid_team != "all":
            filters &= Q(raid_team=int(self.raid_team))

        if self.raid_costume != "all":
            filters &= Q(raid_costume=self.raid_costume)

        if self.raid_is_exclusive != "all":
            filters &= Q(raid_is_exclusive=int(self.raid_is_exclusive))

        if self.raid_ex_raid_eligible != "all":
            filters &= Q(raid_ex_raid_eligible=int(self.raid_ex_raid_eligible))

        try:
            query = models.AggregatedRaids.filter(filters).order_by(
                "gym__gym",
                "raid_pokemon",
                "raid_level",
                "raid_form",
                "raid_team",
                "raid_costume",
                "raid_is_exclusive",
                "raid_ex_raid_eligible",
                "area__name",
                "month_year"
            ).annotate(
                gym_id=F("gym__gym"),
                gym_name=F("gym__gym_name"),
                gym_latitude=F("gym__latitude"),
                gym_longitude=F("gym__longitude"),
                area_name=F("area__name")
            )

            if self.limit > 0:
                query = query.limit(self.limit)

            results = await query.values(
                "gym_id",
                "gym_name",
                "gym_latitude",
                "gym_longitude",
                "raid_pokemon",
                "raid_level",
                "raid_form",
                "raid_team",
                "raid_costume",
                "raid_is_exclusive",
                "raid_ex_raid_eligible",
                "area_name",
                "total_count",
                "month_year"
            )

            logger.info(f"✅ Retrieved {len(results)} raid rows (limit={self.limit}).")
            return {"results": results}
        except Exception as e:
            logger.error(f"❌ Error in raid_sql_data: {e}")
            return {"error": str(e)}
