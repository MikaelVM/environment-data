CREATE TABLE IF NOT EXISTS staging_station(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    owner VARCHAR(255),
    country VARCHAR(255),
    longitude FLOAT,
    latitude FLOAT,
    created TIMESTAMP,
    operation_from TIMESTAMP,
    operation_to TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    updated TIMESTAMP,
    parameter_id VARCHAR(255)[],
    anemometer_height FLOAT,
    barometer_height FLOAT,
    station_height FLOAT
);

CREATE TABLE IF NOT EXISTS raw_station(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    owner VARCHAR(3),
    country VARCHAR(3),
    longitude FLOAT,
    latitude FLOAT,
    created TIMESTAMP,
    operation_from TIMESTAMP,
    operation_to TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    updated TIMESTAMP,
    parameter_id TEXT[],
    anemometer_height FLOAT,
    barometer_height FLOAT,
    station_height FLOAT
);
