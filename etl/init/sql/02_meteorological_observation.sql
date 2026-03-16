CREATE TABLE IF NOT EXISTS staging_meteorological_observation(
    id SERIAL PRIMARY KEY,
    observation_id VARCHAR(255),
    station_id INTEGER,
    station_longitude FLOAT,
    station_latitude FLOAT,
    parameter_id VARCHAR(255),
    value FLOAT,
    created TIMESTAMP,
    observed TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_meteorological_observation(
    id SERIAL PRIMARY KEY,
    observation_id VARCHAR(255),
    station_id INTEGER,
    station_longitude FLOAT,
    station_latitude FLOAT,
    parameter_id VARCHAR(255),
    value FLOAT,
    created TIMESTAMP,
    observed TIMESTAMP
);
