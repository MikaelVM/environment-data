from pathlib import Path
from etl import DatabaseHandler, APIFetcher

import pandas as pd

def init():
    config_file = Path('./configs/local_db_config.ini')
    db_handler = DatabaseHandler(config_file)

def extract():
    api_fetcher = APIFetcher()
    csv_folder = Path('./api_data')

    # Request URL for DMI Station API
    dmi_station_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/station/items'
    station_json_response = api_fetcher.fetch(dmi_station_api_request_url).json()

    # Using pandas to take response as json, normalize it and convert to dataframe, then save it as csv file
    station_data = pd.json_normalize(station_json_response['features'])
    station_data.to_csv(csv_folder / 'station_data.csv', index=False)

    # Request URL for DMI Meteorological Observations API
    dmi_mo_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'
    fetch_limit = 1000

    # Loop to fetch data in batches and save to CSV file until no more data is returned
    offset = 0
    while True:
        mo_json_response = api_fetcher.fetch(dmi_mo_api_request_url, api_parameters={'limit': fetch_limit, 'offset': offset}).json()
        mo_data = pd.json_normalize(mo_json_response['features'])
        if mo_data.empty:
            break
        mo_data.to_csv(csv_folder / 'meteorological_observations_data.csv', mode='a', index=False, header=not (csv_folder / 'meteorological_observations_data.csv').exists())
        offset += fetch_limit

def transform():
    pass

def load():
    pass

if __name__ == "__main__":
    print(f"{'='*20} Starting ETL Process {'='*20}")
    print(f"{'='*20} Initializing the Database{'='*20}")
    init()

    print(f"{'='*20} Extracting data...{'='*20}")
    extract()

    print(f"{'='*20} Transforming data...{'='*20}")
    transform()

    print(f"{'='*20} Loading data into the database...{'='*20}")
    load()


