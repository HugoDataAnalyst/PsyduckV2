"""
Docker healthcheck: exits 0 when /health answers 200, non-zero otherwise.

    python healthcheck.py              # webhook + API app
    python healthcheck.py dashboard    # Dash dashboard

Sends the API secret header / bearer token from .env when they are set.
"""
import sys
import urllib.request

import config as AppConfig


def _local(host: str) -> str:
    return "127.0.0.1" if host in ("", "0.0.0.0", "::") else host


if len(sys.argv) > 1 and sys.argv[1] == "dashboard":
    req = urllib.request.Request(f"http://{_local(AppConfig.dashboard_ip)}:{AppConfig.dashboard_port}/health")
else:
    req = urllib.request.Request(f"http://{_local(AppConfig.golbat_host)}:{AppConfig.golbat_webhook_port}/health")
    if AppConfig.api_header_name and AppConfig.api_header_secret:
        req.add_header(AppConfig.api_header_name, AppConfig.api_header_secret)
    if AppConfig.api_secret_key:
        req.add_header("Authorization", f"Bearer {AppConfig.api_secret_key}")

urllib.request.urlopen(req, timeout=8)   # raises (exit 1) on 503, 403 or no answer
