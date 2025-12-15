# Psyduck V2 - A Data Analyst's Pokemon Experimental Tool

## Configuration
1. Copy the [`.env.example`](./.env.example) file to `.env` and fill in the required environment variables.
2. Copy [config/example.config.json](./config/example.config.json) to `config/config.json` and fill in the required configurations.
3. Copy [dashboard/dashboard_config.example.json](./dashboard/dashboard_config.example.json) to `dashboard/dashboard_config.json` and customize your dashboard settings (map name, navbar links, etc.).
4. Ensure in Golbat you have the webhook with `/webhook`.
Example:
`127.0.0.1:5050/webhook`

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
>
> Change DB_PORT to 3307 if you are using the docker-compose setup and didn't touch the docker-compose.yml file.

## Docker stand alone container options:

1. Run `docker compose up psyduckv2_db` to start the database container
2. Run `docker compose up psyduckv2_pma` to start the phpmyadmin container
3. Run `docker compose up psyduckv2_app` to start the app container

## Redis Tuning (Recommended for Production)

`Redis doesn’t run well on VPS setups that rely on virtual memory. For large-scale webhook feeding into PsyduckV2, it needs guaranteed physical RAM to perform reliably.`

To ensure Redis performs well under high load, apply the following **system-level** and **Redis config** optimizations:

### System-Level Optimizations

Add these to your system's startup or provisioning scripts:

```bash
# Allow memory overcommit (recommended for Redis)
sysctl -w vm.overcommit_memory=1

# Reduce swapping tendency
sysctl -w vm.swappiness=1

# You can persist these directly in:
/etc/sysctl.conf
# Run this after adding it to the sysctl.conf at the end:
sudo sysctl -p

# Disable Transparent Huge Pages (causes latency spikes)
echo never > /sys/kernel/mm/transparent_hugepage/enabled

Redis Configuration:
# Security and access
protected-mode yes              # Keep enabled unless you're running behind strict network rules
bind 127.0.0.1                   # To allow connections from outside you can do 0.0.0.0 for all or set a specific IP you want to allow: bind 192.168.1.100, 127.0.0.1, etc
requirepass YOUR_PASSWORD_HERE # Always set a password in production!

# Memory handling
maxmemory 8gb                 # Optional: Limit memory usage

# TCP stack tuning
tcp-keepalive 0                # Keep connections alive (0 = OS default)
tcp-backlog 65536             # Allow more simultaneous TCP connections

# Persistence
appendonly yes                 # Use AOF (Append Only File) persistence
appendfsync everysec           # Default; sync every second (can be heavy under high writes)
# appendfsync no              # Consider this if using `save` + RDB-only strategy
# You can swap appendfsync no and use:
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 512mb

# Save snapshots
save 900 1                     # Save if at least 1 key changed in 15 min
save 300 10                    # Save if at least 10 keys changed in 5 min
save 60 10000                  # Save if at least 10k keys changed in 1 min

# You'll need to debug your own redis and adjust these as needed.
```

## Development

Database migrations are handled by Alembic

Create an alembic file to create the migration:

```alembic revision -m "my_cool_migration_name"```

Run the migration locally before pushing to the repository:

```alembic upgrade head```


> This command will create a new migration file in the alembic/versions/ directory.
> Make sure to commit the migration file to the repository.

## Dashboard

The dashboard is a separate Dash-based analytics application that provides visual analytics for Pokemon GO events.

### Running the Dashboard

```bash
# Start the dashboard (runs on port 8050 by default)
python run_dashboard.py
```

> **Important:** The dashboard requires the main application (`psyduckv2.py`) to be running, as it fetches data from the API endpoints.

### Features

- **Home**: Landing page with overview statistics
- **Pokemon**: Pokemon spawn analytics with IV distributions, timeseries charts, shiny rates, and heatmaps
- **Raids**: Raid occurrence analytics by level and Pokemon
- **Quests**: Quest analytics by reward type and pokestop
- **Invasions**: Team Rocket invasion analytics by type and location

### Multi-Language Support

The dashboard supports multiple languages:
- English (en)
- Portuguese (pt)
- German (de)
- French (fr)

Switch languages using the flag dropdown in the navbar.

### Background Tasks

The dashboard runs background tasks to periodically fetch and cache data from the API:

- **Daily tasks** (hourly refresh): Areas, Pokestops, Pokemon, Raids, Invasions, Quests
- **Alltime tasks** (daily refresh): Historical aggregations for all event types

On startup, daily tasks run first, followed by "alltime" tasks after a 30-second delay to avoid overwhelming the API.

### Dashboard Customization

The dashboard can be customized via `dashboard/dashboard_config.json`:

```json
{
  "map_name": "YourMapName",
  "icon_url": "/assets/custom_images/your_icon.png",
  "custom_navbar_links": [
    {
      "name": "API Docs",
      "url": "https://your-api-docs-url.com/",
      "icon": "bi bi-book-fill",
      "external": true
    }
  ]
}
```

- `map_name`: Displayed as "PsyduckV2 {map_name}" in navbar and page titles
- `icon_url`: Custom icon displayed between "PsyduckV2" and map name (place images in `dashboard/assets/custom_images/`)
- `custom_navbar_links`: Additional navbar links with [Bootstrap Icons](https://icons.getbootstrap.com/)

**Custom Images:**
Place your custom images in `dashboard/assets/custom_images/` (gitignored). This directory is ignored by git to keep your customizations private.

**Icon Caching:**
The dashboard automatically caches Pokemon and reward icons locally on startup for faster loading. Icons are downloaded from the WatWowMap UICONS repository and stored in `dashboard/assets/` subdirectories (gitignored). First startup may take 2-5 minutes to download all icons; subsequent starts are instant.

## API Documentation

 Available [here](https://docspsyduckv2.databyhugo.com/).
