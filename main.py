from etl.extract.api_fetchers import fetch_stations
import psycopg as psql
from pathlib import Path
from etl.helper_functions import file_to_sql
from etl.init import init_db
from etl.extract import extractor, extract_meteorological_observations
from postgresql_runner import PostgreSQLRunner

if __name__ == "__main__":
    config_file = Path('./configs/local_db_config.ini')
    query_runner = PostgreSQLRunner(config_file, verbose=False)

    # Initialize the database by creating the necessary tables
    # Also, drops existing tables if they exist to ensure a clean slate for demonstration purposes
    init_db(query_runner, verbose=False)

    # Extract data from the API and insert it into the database
    # extract_station(query_runner)
    extract_meteorological_observations(
        Path('./api_data/mo_station_06186.csv'),
        station_id='06186', limit=5
    )

    # transform_station(query_runner)
    # load_station(query_runner)