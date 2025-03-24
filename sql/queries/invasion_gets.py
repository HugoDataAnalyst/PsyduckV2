from datetime import datetime
from utils.logger import logger
from sql import models
from tortoise.expressions import Q

class InvasionSQLQueries():
    def __init__(self, area: str, start: datetime, end: datetime,
                 pokestop_id: str = "all", display_type: str = "all",
                 character: str = "all", grunt: str = "all",
                 confirmed: str = "all", limit: int = 0):
        self.area = area
        self.start = start
        self.end = end
        self.pokestop_id = pokestop_id
        self.display_type = display_type
        self.character = character
        self.grunt = grunt
        self.confirmed = confirmed
        self.limit = limit

    async def invasion_sql_data(self) -> dict:
        filters = Q(month_year__gte=self.start) & Q(month_year__lte=self.end)

        if self.area.lower() != "global":
            filters &= Q(area__name=self.area)

        if self.pokestop_id != "all":
            filters &= Q(pokestop__pokestop=self.pokestop_id)

        if self.display_type != "all":
            filters &= Q(display_type=int(self.display_type))

        if self.character != "all":
            filters &= Q(character=int(self.character))

        if self.grunt != "all":
            filters &= Q(grunt=int(self.grunt))

        if self.confirmed != "all":
            filters &= Q(confirmed=int(self.confirmed))

        try:
            query = models.AggreagatedInvasions.filter(filters).order_by(
                "pokestop__pokestop",
                "display_type",
                "character",
                "grunt",
                "confirmed",
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
                "display_type",
                "character",
                "grunt",
                "confirmed",
                "area__name",
                "total_count",
                "month_year"
            )

            logger.info(f"✅ Retrieved {len(results)} invasion rows (limit={self.limit}).")
            return {"results": results}
        except Exception as e:
            logger.error(f"❌ Error in invasion_sql_data: {e}")
            return {"error": str(e)}
