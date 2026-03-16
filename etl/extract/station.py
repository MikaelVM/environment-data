from pathlib import Path

from .api_fetchers import fetch_stations
from postgresql_runner import PostgreSQLRunner
from etl.helper_functions import file_to_sql
from rich.progress import Progress

def extract_station(
        query_runner: PostgreSQLRunner,
        *,
        station_id: str = None,
        fetch_limit: int =500,
        verbose: bool = True
):
    reached_end = False
    offset = 0
    while not reached_end:
        api_response = fetch_stations(station_id=station_id, limit=fetch_limit, offset=offset)

        print(f"Fetched {len(api_response)} station records from API:") if verbose else None

        if len(api_response) < fetch_limit:
            reached_end = True
        else:
            offset += fetch_limit

        with Progress() as progress:
            task = progress.add_task("Inserting station data into database...", total=len(api_response))

            for row in api_response:
                progress.update(task, advance=1)
                sql = file_to_sql(Path(__file__).parent / 'sql' / 'station.sql')
                query_runner.run_query(
                    sql,
                    [
                        row['name'],
                        row['owner'],
                        row['country'],
                        row['longitude'],
                        row['latitude'],
                        row['created'],
                        row['operation_from'],
                        row['operation_to'],
                        row['valid_from'],
                        row['valid_to'],
                        row['updated'],
                        row['parameter_id'],
                        row['anemometer_height'],
                        row['barometer_height'],
                        row['station_height']
                    ])

    print("Finished extracting station data and inserting into database.") if verbose else None