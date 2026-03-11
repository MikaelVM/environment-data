# environment-data
Project with focus on the ETL process of processing environment data

# OLD CODE 

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)


    query_runner = psql.connect(
        host='localhost',
        port=5432,
        dbname='environment',
        user='postgres',
        password='pass'
    ).cursor()

    for e in fetch_stations(station_id='06072', limit=5):
        query = f"""
            INSERT INTO staging_station(
                name, owner, country, longitude, latitude, created, operation_from, operation_to, valid_from, valid_to, 
                updated, parameter_id, anemometer_height, barometer_height, station_height
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s 
            );
            """
        values = (
            e['name'], e['owner'], e['country'], e['longitude'], e['latitude'], e['created'], e['operation_from'],
            e['operation_to'], e['valid_from'], e['valid_to'], e['updated'], e['parameter_id'], e['anemometer_height'],
            e['barometer_height'], e['station_height']
        )
        print(query)
        query_runner.execute(query, values)
        query_runner.connection.commit()
    query_runner.close()
---

    if len(api_response) == 0:
        if verbose:
            print("No station data found to extract.")
        return