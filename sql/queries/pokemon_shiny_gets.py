from datetime import datetime
from utils.logger import logger
from sql import models
from tortoise.expressions import Q

class ShinySQLQueries():
    def __init__(self, area: str, start: datetime, end: datetime,
                 username: str = "all", pokemon_id: str = "all",
                 form: str = "all", shiny: str = "all", limit: int = 0):
        self.area = area
        self.start = start
        self.end = end
        self.username = username
        self.pokemon_id = pokemon_id
        self.form = form
        self.shiny = shiny
        self.limit = limit

    async def shiny_sql_rates(self) -> dict:
        filters = Q(month_year__gte=self.start) & Q(month_year__lte=self.end)

        if self.area.lower() != "global":
            filters &= Q(area__name=self.area)

        if self.username != "all":
            filters &= Q(username=self.username)

        if self.pokemon_id != "all":
            filters &= Q(pokemon_id=int(self.pokemon_id))

        if self.form != "all":
            filters &= Q(form=self.form)

        if self.shiny != "all":
            filters &= Q(shiny=int(self.shiny))

        try:
            query = models.ShinyUsernameRates.filter(filters).order_by(
                "username",
                "pokemon_id",
                "form",
                "shiny",
                "area__name",
                "month_year"
            )

            if self.limit > 0:
                query = query.limit(self.limit)

            results = await query.values(
                "username",
                "pokemon_id",
                "form",
                "shiny",
                "area__name",
                "total_count",
                "month_year"
            )

            logger.info(f"✅ Retrieved {len(results)} shiny rate rows (limit={self.limit}).")
            return {"results": results}
        except Exception as e:
            logger.error(f"❌ Error in shiny_sql_rates: {e}")
            return {"error": str(e)}

