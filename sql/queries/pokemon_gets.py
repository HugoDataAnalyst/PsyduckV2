from datetime import datetime
from utils.logger import logger
from sql import models
from tortoise.expressions import Q, F

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
                "spawnpoint",
                "month_year"
            ).annotate(area_name=F("area_name"))

            if self.limit > 0:
                query = query.limit(self.limit)

            # Only call .values() after .limit() is applied
            results = await query.values(
                "pokemon_id",
                "form",
                "iv",
                "area_name",
                "spawnpoint",
                "total_count",
                "month_year"
            )

            # Obtain spawnpoint details
            spawnpoint_values = {r["spawnpoint"] for r in results}
            spawnpoints = await models.Spawnpoint.filter(spawnpoint__in=list(spawnpoint_values)).values("spawnpoint", "latitude", "longitude")
            spawnpoint_map = {sp["spawnpoint"]: sp for sp in spawnpoints}

            # Attach the results with latitude and longitude.
            for record in results:
                sp_data = spawnpoint_map.get(record["spawnpoint"])
                record["latitude"] = sp_data["latitude"] if sp_data else None
                record["longitude"] = sp_data["longitude"] if sp_data else None

            logger.info(f"✅ Retrieved {len(results)} heatmap rows (limit={self.limit}).")
            return {"results": results}
        except Exception as e:
            logger.error(f"❌ Error in pokemon_sql_heatmap: {e}")
            return {"error": str(e)}

