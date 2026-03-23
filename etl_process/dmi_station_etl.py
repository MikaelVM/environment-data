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
from etl import DatabaseHandler, APIFetcher, ETLProcess
from helper_functions import JSONHandler

class DMIStationETLProcess(ETLProcess):
    def __init__(self):
        super().__init__()
        self.api_fetcher = APIFetcher()
        self.json_handler = JSONHandler()
        self.data_folder = Path('../data_folder')
        self.dmi_station_file = self.data_folder / 'dmi_stations'

    def process_name(self) -> str:
        return "DMI Station ETL Process"

    def init(self):
        pass

    def extract(self):
        # Fetch data from the DMI API for station information and save the JSON response to a file.
        self.console.log("Fetching station data from DMI API...", style="yellow")
        with self.console.status("Fetching station data from DMI API..."):
            dmi_station_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/station/items'

            self.json_handler.write_json(
                file_path=self.dmi_station_file.with_suffix('.json'),
                data=self.api_fetcher.fetch(dmi_station_api_request_url).json()
            )
        self.console.log("Station data fetched and saved successfully.", style="green")

    def transform(self):
        self.console.log("Transforming station data...", style="yellow")

        self.console.log(
            "Reading station data from JSON file and normalizing it into a DataFrame.",
            style="blue"
        )

        json_data = self.json_handler.read_json(file_path=self.dmi_station_file.with_suffix('.json'))

        """
        Normalization happens on the 'features' key of the original JSON data, which contains a list of station data.
        Each station is represented as a dictionary with various properties.
        """

        dmi_stations_dataframe = pd.json_normalize(json_data['features'])

        self.console.log("Station data read and normalized successfully.", style="green")
        self.console.log("Transforming station data into a format suitable for analysis.", style="yellow")

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

        #TODO: Further transformations, such as handling missing values, converting data types, etc.

        self.console.log("Station data transformed successfully.", style="green")

        dmi_stations_dataframe.to_csv(self.dmi_station_file.with_suffix('.csv'))


    def load(self):
        self.console.log("Loading station data into the database...", style="yellow")

        # TODO: Implement loading of the transformed station data into the database.

        self.console.log("Loading station data into the database is not implemented yet.", style="red")



if __name__ == "__main__":
    etl_process = DMIStationETLProcess()
    etl_process.run()


