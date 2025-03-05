# Psyduck V2 - A Data Analyst's Pokemon Experimental Tool

## Docker Setup
1. Install Docker Desktop
2. `docker network create psyduckv2_network`
3. `docker compose up -d`

### If you want to run python natively:

1. Install Python 3.11
2. `python3 -m venv .venv`
3. `source .venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python3 psyduckv2.py`

> This option requires you to have a MySQL database running on your local/external machine and manually set the DB_HOST environment variable to the correct IP address.

## Docker stand alone container options:

1. Run `docker compose up psyduckv2_db` to start the database container
2. Run `docker compose up psyduckv2_pma` to start the phpmyadmin container
3. Run `docker compose up psyduckv2_app` to start the app container
