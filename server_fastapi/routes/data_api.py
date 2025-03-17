import config as AppConfig
from utils import secure_api
from my_redis.connect_redis import RedisManager  # Assuming this module has the required methods
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from typing import Optional
router = APIRouter()

@router.get(
    "/api/redis/total_pokemons_hourly",
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_total_pokemons_hourly(
    # These parameters will now show up in the docs for manual input
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Manually perform the secret header and secret key validations
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)
    message = "This is working if it passes the checks"
    return message
