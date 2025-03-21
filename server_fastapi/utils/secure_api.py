import config as AppConfig
from fastapi import Request, HTTPException, Header, Query
from utils.logger import logger
from typing import Optional
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# Optional parameter. Validate Remote Address
async def validate_remote_addr(request: Request):
    if not AppConfig.validated_remote_address:
        logger.info(f"⏭️ No validation set. Skipping...")
        return
    remote_addr = request.client.host
    if remote_addr != AppConfig.validated_remote_address:
        logger.warning(f"⚠️ Invalid remote address: {remote_addr}. Skipping...")
        raise HTTPException(status_code=403, detail="❌ Invalid remote address.")

# Define allowed paths for the API
ALLOWED_PATHS = [
    "/static/psyduck.ico",
    "/static/psyduck.webp",
    "/static/psyduck-flex.gif",
    "/webhook",
    "/docs", # Allow default docs FastAPI page
    "/api/redis/total_pokemons_hourly",
    "/api/redis/get_pokemon_counterseries",
    "/api/redis/get_raids_counterseries",
    "/api/redis/get_invasions_counterseries",
    "/api/redis/get_quest_counterseries",
    "/api/redis/get_pokemon_timeseries",
    "/api/redis/get_invasion_timeseries"
]

async def validate_path(request: Request):
    path = request.url.path
    if path not in ALLOWED_PATHS:
        logger.warning(f"⚠️ Path not allowed: {path}. Skipping...")
        raise HTTPException(status_code=403, detail="❌ Path not allowed.")
    logger.info(f"✅ Path allowed: {path}")
    return

async def validate_ip(request: Request):
    allowed_ips = AppConfig.allowed_ips
    if allowed_ips:
        client_ip = request.client.host
        if client_ip not in AppConfig.allowed_ips:
            logger.warning(f"⚠️ IP not allowed: {client_ip}")
            raise HTTPException(status_code=403, detail="❌ IP not allowed")
        logger.info(f"✅ IP {client_ip} is in the allowed list.")
        return
    else:
        logger.info(f"⏭️ All connections are allowed. Skipping check...")
        return

async def check_secret_header_value(header_value: Optional[str]):
    header_name = AppConfig.api_header_name
    header_secret = AppConfig.api_header_secret
    if header_name and header_secret:
        if header_value != header_secret:
            logger.warning(f"⚠️ Invalid secret header value: {header_value}")
            raise HTTPException(status_code=403, detail="Invalid secret header")
        logger.info("✅ Secret Header Validated.")
    else:
        logger.info("⏭️ No Header option set. Skipping...")

async def check_secret_key_value(key_value: Optional[str]):
    secret_key = AppConfig.api_secret_key
    if secret_key:
        if key_value != secret_key:
            logger.warning(f"⚠️ Invalid API secret key provided: {key_value}")
            raise HTTPException(status_code=403, detail="Invalid API secret key")
        logger.info("✅ Secret Key Validated")
    else:
        logger.info("⏭️ No Secret Key set. Skipping...")

# New helper functions to define parameters based on AppConfig

def get_secret_header_param():
    """
    Returns a Header parameter based on config.
    If api_header_name and api_header_secret are defined, the header is required.
    Otherwise, it's optional.
    """
    if AppConfig.api_header_name and AppConfig.api_header_secret:
        return Header(..., description="Enter your API secret header", alias=AppConfig.api_header_name)
    else:
        return Header(None, description="No secret header required")

def get_secret_key_param():
    """
    Returns a Query parameter for the secret key based on config.
    If api_secret_key is defined, the parameter is required.
    Otherwise, it's optional.
    """
    if AppConfig.api_secret_key:
        return Query(..., description="Enter your API secret key")
    else:
        return Query(None, description="No secret key required")

# Custom Middleware to Secure API Endpoints
class AllowedPathsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Special handling for /openapi.json:
        if request.url.path == "/openapi.json":
            referer = request.headers.get("referer", "")
            if "/docs" not in referer:
                logger.warning("Direct access to /openapi.json is not allowed")
                return JSONResponse(
                    {"detail": "Access to OpenAPI schema is not allowed."},
                    status_code=403
                )
        # For all other paths, check if the path is in ALLOWED_PATHS.
        elif request.url.path not in ALLOWED_PATHS:
            logger.warning(f"Path not allowed: {request.url.path}")
            return JSONResponse(
                {"detail": "Path not allowed."},
                status_code=403
            )
        response = await call_next(request)
        return response
