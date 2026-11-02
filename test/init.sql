-- Runs once when the test MariaDB container initialises.
-- PsyduckV2's own schema is created by Alembic on app startup; this only
-- provides the minimal Golbat database the pokestop refresh queries.

CREATE DATABASE IF NOT EXISTS golbat CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS golbat.pokestop (
    id   VARCHAR(35) NOT NULL PRIMARY KEY,
    lat  DOUBLE NOT NULL,
    lon  DOUBLE NOT NULL,
    name VARCHAR(128) NULL
) ENGINE=InnoDB;

INSERT INTO golbat.pokestop (id, lat, lon, name) VALUES
    ('stop-a', 38.705, -9.245, 'Stop A'),
    ('stop-b', 38.725, -9.205, 'Stop B'),
    ('stop-c', 38.745, -9.165, 'Stop C');

-- Golbat's weather table (same columns as Golbat's schema)
CREATE TABLE IF NOT EXISTS golbat.weather (
    id BIGINT NOT NULL PRIMARY KEY,
    level TINYINT UNSIGNED NULL,
    latitude DOUBLE NOT NULL DEFAULT 0,
    longitude DOUBLE NOT NULL DEFAULT 0,
    gameplay_condition TINYINT UNSIGNED NULL,
    wind_direction MEDIUMINT NULL,
    cloud_level TINYINT UNSIGNED NULL,
    rain_level TINYINT UNSIGNED NULL,
    wind_level TINYINT UNSIGNED NULL,
    snow_level TINYINT UNSIGNED NULL,
    fog_level TINYINT UNSIGNED NULL,
    special_effect_level TINYINT UNSIGNED NULL,
    severity TINYINT UNSIGNED NULL,
    warn_weather TINYINT UNSIGNED NULL,
    updated INT UNSIGNED NOT NULL
) ENGINE=InnoDB;

GRANT ALL PRIVILEGES ON golbat.* TO 'psyduck'@'%';
FLUSH PRIVILEGES;
