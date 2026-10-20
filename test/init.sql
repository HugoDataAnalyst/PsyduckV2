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

GRANT ALL PRIVILEGES ON golbat.* TO 'psyduck'@'%';
FLUSH PRIVILEGES;
