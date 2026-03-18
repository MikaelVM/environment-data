import textwrap

import pandas as pd
from pathlib import Path
import json
import rich
from psycopg.sql import SQL

from etl import DatabaseHandler, APIFetcher
from helper_functions import JSONHandler

DATA_FOLDER = Path('data_folder')
DMI_STATION_FILE = DATA_FOLDER / 'dmi_stations.json'


def init_db():
    config_file = Path('./configs/local_db_config.ini')
    sql_folder = Path('./sql/init')
    db_handler = DatabaseHandler(config_file)

    for sql_file in sql_folder.glob('*.sql'):
        print_centered_message(f" Running initialization script: {sql_file.name} ", fillchar='-')
        db_handler.run_query(sql_file.read_text())

def extract():
    api_fetcher = APIFetcher()
    json_handler = JSONHandler()

    # Fetch data from the DMI API for station information and save the JSON response to a file.
    dmi_station_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/station/items'

    json_handler.write_json(
        file_path=DATA_FOLDER / 'dmi_stations.json',
        data=api_fetcher.fetch(dmi_station_api_request_url).json()
    )

    # Fetch data from the DMI API for meteorological observations for station 06180 and save the JSON responses
    # to files organized by station, year, month, and day.
    dmi_mo_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'
    observation_folder = DATA_FOLDER / 'observations'

    from helper_functions import construct_datetime_argument
    from datetime import datetime
    import calendar
    from rich.progress import track


    years = [2025]
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    stations = ['06180']
    limit = 100000

    for station in stations:
        for year in years:
            for month in months:
                for day in track(
                        range(1, calendar.monthrange(year, month)[1] + 1),
                        description=f"Fetching data for station {station}, year {year}, month {month}"
                ):

                    api_params = {
                        'stationId': station,
                        'datetime': construct_datetime_argument(datetime(year, month, day, 0, 0, 0,), datetime(year, month, day, 23, 59, 59)),
                        'limit': limit,
                    }

                    while True:
                        data = api_fetcher.fetch(dmi_mo_api_request_url, api_parameters=api_params).json()

                        if data['numberReturned'] == 0:
                            break

                        json_handler.write_json(
                            file_path=observation_folder / f'station_{station}' / f'{year}' / f'{month:02d}' /f'day_{day:02d}_part_{api_params.get("offset", 0) // limit}.json',
                            data=data,
                            makedir=True
                        )

                        api_params['offset'] = api_params.get('offset', 0) + limit



def transform():
    json_handler = JSONHandler()

    pass


def load():
    pass

def print_centered_message(message: str, fillchar: str = '=', width: int = 80):
    print(textwrap.fill(message.center(width, fillchar), width=width))

if __name__ == "__main__":
    print_centered_message(' Starting ETL Process ')

    print_centered_message(' Extracting Data from APIs ')
    extract()

    print_centered_message(' Transforming Data ')
    transform()

    print_centered_message(' Initializing the Database ')
    init_db()

    print_centered_message(' Loading Data ')
    load()


