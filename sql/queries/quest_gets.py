from datetime import datetime
from utils.logger import logger
from sql import models
from tortoise.expressions import Q

class QuestSQLQueries():
    def __init__(self, area: str, start: datetime, end: datetime,
                 pokestop_id: str = "all", ar_type: str = "all",
                 normal_type: str = "all", reward_ar_type: str = "all",
                 reward_normal_type: str = "all",
                 reward_ar_item_id: str = "all",
                 reward_normal_item_id: str = "all",
                 reward_ar_poke_id: str = "all",
                 reward_normal_poke_id: str = "all",
                 limit: int = 0):
        self.area = area
        self.start = start
        self.end = end
        self.pokestop_id = pokestop_id
        self.ar_type = ar_type
        self.normal_type = normal_type
        self.reward_ar_type = reward_ar_type
        self.reward_normal_type = reward_normal_type
        self.reward_ar_item_id = reward_ar_item_id
        self.reward_normal_item_id = reward_normal_item_id
        self.reward_ar_poke_id = reward_ar_poke_id
        self.reward_normal_poke_id = reward_normal_poke_id
        self.limit = limit

    async def quest_sql_data(self) -> dict:
        filters = Q(month_year__gte=self.start) & Q(month_year__lte=self.end)

        if self.area.lower() != "global":
            filters &= Q(area__name=self.area)

        if self.pokestop_id != "all":
            filters &= Q(pokestop__pokestop=self.pokestop_id)

        if self.ar_type != "all":
            filters &= Q(ar_type=int(self.ar_type))

        if self.normal_type != "all":
            filters &= Q(normal_type=int(self.normal_type))

        if self.reward_ar_type != "all":
            filters &= Q(reward_ar_type=int(self.reward_ar_type))

        if self.reward_normal_type != "all":
            filters &= Q(reward_normal_type=int(self.reward_normal_type))

        if self.reward_ar_item_id != "all":
            filters &= Q(reward_ar_item_id=int(self.reward_ar_item_id))

        if self.reward_normal_item_id != "all":
            filters &= Q(reward_normal_item_id=int(self.reward_normal_item_id))

        if self.reward_ar_poke_id != "all":
            filters &= Q(reward_ar_poke_id=int(self.reward_ar_poke_id))

        if self.reward_normal_poke_id != "all":
            filters &= Q(reward_normal_poke_id=int(self.reward_normal_poke_id))

        try:
            query = models.AggregatedQuests.filter(filters).order_by(
                "pokestop__pokestop",
                "ar_type",
                "normal_type",
                "reward_ar_type",
                "reward_normal_type",
                "reward_ar_item_id",
                "reward_normal_item_id",
                "reward_ar_poke_id",
                "reward_normal_poke_id",
                "area__name",
                "month_year"
            )

            if self.limit > 0:
                query = query.limit(self.limit)

            results = await query.values(
                "pokestop__pokestop",
                "pokestop__pokestop_name",
                "pokestop__latitude",
                "pokestop__longitude",
                "ar_type",
                "normal_type",
                "reward_ar_type",
                "reward_normal_type",
                "reward_ar_item_id",
                "reward_ar_item_amount",
                "reward_normal_item_id",
                "reward_normal_item_amount",
                "reward_ar_poke_id",
                "reward_ar_poke_form",
                "reward_normal_poke_id",
                "reward_normal_poke_form",
                "area__name",
                "total_count",
                "month_year"
            )

            logger.info(f"✅ Retrieved {len(results)} quest rows (limit={self.limit}).")
            return {"results": results}
        except Exception as e:
            logger.error(f"❌ Error in quest_sql_data: {e}")
            return {"error": str(e)}
