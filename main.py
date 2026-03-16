from etl.extract.api_fetchers import fetch_stations
import psycopg as psql
from pathlib import Path
from etl.helper_functions import file_to_sql
from etl.init import init_db
from etl.extract import extract_station, extract_meteorological_observations,extract_meteorological_observations_to_csv
from postgresql_runner import PostgreSQLRunner

if __name__ == "__main__":
    config_file = Path('./local_config.ini')
    query_runner = PostgreSQLRunner(config_file, verbose=False)

    # Initialize the database by creating the necessary tables
    # Also, drops existing tables if they exist to ensure a clean slate for demonstration purposes
    init_db(query_runner, verbose=False)

    # Extract data from the API and insert it into the database
    # extract_station(query_runner)
    # extract_meteorological_observations(query_runner, station_id='06186')
    extract_meteorological_observations_to_csv(
        Path('temp_data/meteorological_observations.csv'),
        station_id='06186', fetch_limit=100000
    )

    # transform_station(query_runner)
    # load_station(query_runner)