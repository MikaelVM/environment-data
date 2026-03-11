from etl.extract.api_fetchers import fetch_stations
import psycopg as psql
from pathlib import Path
from etl.helper_functions import file_to_sql
from etl.init import init_db
from etl.extract import extract_station
from postgres_runner import PostgreSQLRunner

if __name__ == "__main__":
    config_file = Path('./local_config.ini')
    query_runner = PostgreSQLRunner(config_file, False)

    init_db(query_runner, verbose=False)
    extract_station(query_runner)
    # transform_station(query_runner)
    # load_station(query_runner)