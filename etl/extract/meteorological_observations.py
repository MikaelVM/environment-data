from pathlib import Path
import csv

from .api_fetchers import fetch_meteorological_observations
from postgresql_runner import PostgreSQLRunner
from etl.helper_functions import file_to_sql
from rich.progress import Progress

# TODO: Find better ways to pass parameters to the API fetcher function, e.g. by using a specific param variable to
#  pass any parameters to the API fetcher function, instead of hardcoding specific parameters in the extract function.
#  This would make the extract function more flexible and reusable for different API fetchers with different parameters.
def extract_meteorological_observations(
        query_runner: PostgreSQLRunner,
        *,
        station_id: str = None,
        limit: int = None,
        fetch_limit: int =500,
        verbose: bool = True
):
    if limit and limit < fetch_limit:
        fetch_limit = limit

    reached_end = False
    fetched_records = 0
    offset = 0

    while not reached_end:
        api_response = fetch_meteorological_observations(
            station_id=station_id,
            limit=fetch_limit,
            offset=offset
        )

        print(f"Fetched {len(api_response)} meteorological observation records from API:") if verbose else None

        if len(api_response) < fetch_limit:
            reached_end = True
        else:
            offset += fetch_limit

        fetched_records += len(api_response)

        if limit and fetched_records >= limit:
            print(f"Reached fetch limit of {limit} records. Stopping further API calls.") if verbose else None
            reached_end = True

        with Progress() as progress:
            task = progress.add_task("Inserting meteorological observation data into database...", total=len(api_response))

            for row in api_response:
                progress.update(task, advance=1)
                sql = file_to_sql(Path(__file__).parent / 'sql' / 'meteorological_observation.sql')
                query_runner.run_query(
                    sql,
                    [
                        row['id'],
                        row['station_id'],
                        row['longitude'],
                        row['latitude'],
                        row['parameter_id'],
                        row['value'],
                        row['created'],
                        row['observed']
                    ])

    print("Finished extracting meteorological observation data and inserting into database.") if verbose else None

def extract_meteorological_observations_to_csv(
        file_path: Path,
        *,
        station_id: str = None,
        limit: int = None,
        fetch_limit: int =500,
        verbose: bool = True
):
    # Check if folder exists, if not, raise an error
    if not file_path.parent.exists():
        raise FileNotFoundError(f"The folder {file_path.parent} does not exist. Please create it before running this function.")

    # If the file does not exist, create it, else clear its contents
    if not file_path.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()
    else:
        file_path.write_text('')

    if limit and limit < fetch_limit:
        fetch_limit = limit

    reached_end = False
    fetched_records = 0
    offset = 0

    while not reached_end:
        api_response = fetch_meteorological_observations(
            station_id=station_id,
            limit=fetch_limit,
            offset=offset
        )

        print(f"Fetched {len(api_response)} meteorological observation records from API:") if verbose else None

        if len(api_response) < fetch_limit:
            reached_end = True
        else:
            offset += fetch_limit

        fetched_records += len(api_response)

        if limit and fetched_records >= limit:
            print(f"Reached fetch limit of {limit} records. Stopping further API calls.") if verbose else None
            reached_end = True

        with Progress() as progress:
            task = progress.add_task("Inserting meteorological observation data into database...", total=len(api_response))

            with open(file_path, mode='a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['id', 'station_id', 'longitude', 'latitude', 'parameter_id', 'value', 'created', 'observed']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if csvfile.tell() == 0:  # Write header only if file is empty
                    writer.writeheader()

                for row in api_response:
                    writer.writerow({
                        'id': row['id'],
                        'station_id': row['station_id'],
                        'longitude': row['longitude'],
                        'latitude': row['latitude'],
                        'parameter_id': row['parameter_id'],
                        'value': row['value'],
                        'created': row['created'],
                        'observed': row['observed']
                    })

                    progress.update(task, advance=1)

    print("Finished extracting meteorological observation data and inserting into CSV file.") if verbose else None