from typing import Any
from pathlib import Path
import json
from datetime import datetime
import calendar
from concurrent.futures import ThreadPoolExecutor

from psycopg.sql import SQL
from rich.console import Console
from helper_functions import construct_datetime_argument
import pandas as pd
from etl import DatabaseHandler, APIFetcher
from helper_functions import JSONHandler

DATA_FOLDER = Path('data_folder')
DMI_STATION_FILE = DATA_FOLDER / 'dmi_stations'
DMI_OBSERVATION_FOLDER = DATA_FOLDER / 'dmi_observations'


def init_db():
    console_init = Console()
    console_init.log("Initializing the database...", style="yellow")

    config_file = Path('./configs/local_db_config.ini')
    sql_folder = Path('./sql/init')
    db_handler = DatabaseHandler(config_file)

    for sql_file in sql_folder.glob('*.sql'):
        db_handler.run_query(sql_file.read_text())
    console_init.log("Database initialized successfully.", style="green")

def extract():
    api_fetcher = APIFetcher()
    json_handler = JSONHandler()
    console_extract = Console()

    # Fetch data from the DMI API for station information and save the JSON response to a file.
    console_extract.log("Fetching station data from DMI API...", style="yellow")
    with console_extract.status("Fetching station data from DMI API..."):
        dmi_station_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/station/items'

        json_handler.write_json(
            file_path=DATA_FOLDER / 'dmi_stations.json',
            data=api_fetcher.fetch(dmi_station_api_request_url).json()
        )
    console_extract.log("Station data fetched and saved successfully.", style="green")

    # Fetch data from the DMI API for meteorological observations for station 06180 and save the JSON responses
    # to files organized by station, year, month, and day.
    dmi_mo_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'

    years = [2025]
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    stations = ['06180']
    limit = 100000

    for station in stations:
        for year in years:
            with ThreadPoolExecutor(max_workers=4) as executor:
                for month in months:
                    executor.submit(_fetch_month, api_fetcher, dmi_mo_api_request_url, json_handler, limit, month,
                                    DMI_OBSERVATION_FOLDER, station, year)
    console_extract.log("Finished fetching meteorological observation (MO) data.", style="green")

def _fetch_month(api_fetcher: APIFetcher, dmi_mo_api_request_url: str, json_handler: JSONHandler, limit: int,
                 month: int | Any, observation_folder: Path, station: str, year: int):

    fetch_month_console = Console()
    fetch_month_console.log(f"Fetching MO data for station {station}, year {year}, month {month}...", style="yellow")
    for day in range(1, calendar.monthrange(year, month)[1] + 1):

        api_params = {
            'stationId': station,
            'datetime': construct_datetime_argument(datetime(year, month, day, 0, 0, 0, ),
                                                    datetime(year, month, day, 23, 59, 59)),
            'limit': limit,
        }

        while True:
            data = api_fetcher.fetch(dmi_mo_api_request_url, api_parameters=api_params).json()

            if data['numberReturned'] == 0:
                break

            json_handler.write_json(
                file_path=observation_folder / f'station_{station}' / f'{year}' / f'{month:02d}' /
                          f'day_{day:02d}_part_{api_params.get("offset", 0) // limit + 1}.json',
                data=data,
                makedir=True
            )

            api_params['offset'] = api_params.get('offset', 0) + limit
    fetch_month_console.log(f"MO data for station {station}, year {year}, month {month} "
                            f"fetched and saved successfully.", style="green")


def transform():
    json_handler = JSONHandler()
    console_transform = Console()

    console_transform.log("Transforming station data...", style="yellow")

    console_transform.log(
        "Reading station data from JSON file and normalizing it into a DataFrame.",
        style="blue"
    )

    json_data = json_handler.read_json(file_path=DMI_STATION_FILE.with_suffix('.json'))

    """
    Normalization happens on the 'features' key of the original JSON data, which contains a list of station data.
    Each station is represented as a dictionary with various properties.
    """

    dmi_stations_dataframe = pd.json_normalize(json_data['features'])

    console_transform.log("Station data read and normalized successfully.", style="green")
    console_transform.log("Transforming station data into a format suitable for analysis.", style="yellow")
    
    """
    The 'geometry.coordinates' column contains a list of two values for a point, representing the longitude and 
    latitude of the station. 
    Having lists in columns can make analysis more difficult, so the column is split into two separate columns.
    """
    dmi_stations_dataframe[['longitude', 'latitude']] = dmi_stations_dataframe['geometry.coordinates'].to_list

    """
    Dropping columns that are unnecessary for analysis of the station data.
    - Type: Is 'feature' for all rows. Simply indicates that the row represents a feature, which is useful in the
        original JSON format, but can be inferred from this point on. 
    - geometry.type: The type of the geometry. Observed to be 'Point' for all rows. As all geometries represents the 
        location of a station, this column does not provide any additional information and can be dropped.
    - geometry.coordinates: The original coordinates column, which we have already split into separate longitude and
        latitude columns.
    """
    dmi_stations_dataframe.drop(columns=['type', 'geometry.type', 'geometry.coordinates'], inplace=True)

    """
    Renaming columns to more descriptive names and to follow a consistent naming convention.
    Mostly removing the 'properties.' prefix, which comes from the normalization of the original JSON data.
    """
    dmi_stations_dataframe.rename(columns={
        'id': 'id_string',
        'properties.stationId': 'station_id',
        'properties.wmoStationId': 'wmo_station_id',
        'properties.regionId': 'region_id',
        'properties.name': 'name',
        'properties.owner': 'owner',
        'properties.county': 'county',
        'properties.wmoCountryCode': 'wmo_county_code',
        'properties.anemometerHeight': 'anemometer_height',
        'properties.barometerHeight': 'barometer_height',
        'properties.stationHeight': 'station_height',
        'properties.operationFrom': 'operation_from',
        'properties.parameterId': 'parameter_id',
        'properties.created': 'created',
        'properties.validFrom': 'valid_from',
        'properties.validTo': 'valid_to',
        'properties.operationTo': 'operation_to',
        'properties.type': 'type',
        'properties.name': 'name',
        'properties.updated': 'updated',
        'properties.status': 'status'
    }, inplace=True)

    # Cast columns to the correct data types
    print(dmi_stations_dataframe.columns)
    with pd.option_context('display.max_rows', 3, 'display.max_columns', None):
        print(dmi_stations_dataframe)

    console_transform.log("Station data transformed successfully.", style="green")

    dmi_stations_dataframe.to_csv(DMI_STATION_FILE.with_suffix('.csv'))

def load():
    pass

if __name__ == "__main__":
    console_main = Console()
    console_main.log("Starting the ETL process")

    #console_main.log("Step 1: Extracting data from APIs")
    #extract()

    console_main.log("Step 2: Transforming data")
    transform()

    console_main.log("Step 3: Loading data into the database")
    console_main.log("Initializing the database...")
    init_db()

    console_main.log("Loading data into the database...")
    load()


