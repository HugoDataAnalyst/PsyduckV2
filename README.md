# Psyduck V2 - A Data Analyst's Pokemon Experimental Tool

## Configuration
1. Copy the [`.env.example`](./.env.example) file to `.env` and fill in the required environment variables.
2. Copy [config/example.config.json](./config/example.config.json) to `config/config.json` and fill in the required configurations.


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

## Development

Database migrations are handled by Tortoise ORM with Aerich.

After making changes to the models, run the following command to apply the migrations:

```aerich migrate --name "name_your_migration"```

> This command will create a new migration file in the migrations/models directory.
> Make sure to commit the migration file to the repository.

## API Documentation

 Available [here](https://docspsyduckv2.databyhugo.com/).
