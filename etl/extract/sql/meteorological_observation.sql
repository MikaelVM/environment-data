INSERT INTO
    staging_meteorological_observation(

    observation_id,
    station_id,
    station_longitude,
    station_latitude,
    parameter_id,
    value,
    created,
    observed
    )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
