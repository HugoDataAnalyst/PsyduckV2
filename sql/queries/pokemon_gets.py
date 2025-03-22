from datetime import datetime
from utils.logger import logger
from sql import models
from tortoise.expressions import Q

class PokemonSQLQueries():
    def __init__(self, area: str, start: datetime, end: datetime,
                 pokemon_id: str = "all", form: str = "all",
                 iv_bucket: str = "all", limit: int = 0):
        self.area = area
        self.start = start
        self.end = end
        self.pokemon_id = pokemon_id
        self.form = form
        self.iv_bucket = iv_bucket
        self.limit = limit

    async def pokemon_sql_heatmap(self) -> dict:
        filters = Q(month_year__gte=self.start) & Q(month_year__lte=self.end)

        if self.area.lower() != "global":
            filters &= Q(area__name=self.area)

        if self.pokemon_id != "all":
            filters &= Q(pokemon_id=int(self.pokemon_id))

        if self.form != "all":
            filters &= Q(form=self.form)

        if self.iv_bucket != "all":
            filters &= Q(iv=int(self.iv_bucket))

        try:
            query = models.AggregatedPokemonIVMonthly.filter(filters).order_by(
                "pokemon_id",
                "form",
                "iv",
                "area__name",
                "spawnpoint__latitude",
                "spawnpoint__longitude",
                "month_year"
            )

            if self.limit > 0:
                query = query.limit(self.limit)

            # Only call .values() after .limit() is applied
            results = await query.values(
                "pokemon_id",
                "form",
                "iv",
                "area__name",
                "spawnpoint__latitude",
                "spawnpoint__longitude",
                "total_count",
                "month_year"
            )

            results = await query
            logger.info(f"✅ Retrieved {len(results)} heatmap rows (limit={self.limit}).")
            return {"results": results}
        except Exception as e:
            logger.error(f"❌ Error in pokemon_sql_heatmap: {e}")
            return {"error": str(e)}

