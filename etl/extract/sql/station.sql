INSERT INTO
    staging_station(
                    name, owner, country, longitude, latitude, created, operation_from, operation_to, valid_from,
                    valid_to, updated, parameter_id, anemometer_height, barometer_height, station_height
            )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
