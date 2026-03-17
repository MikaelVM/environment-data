import csv
from typing import Any

from .api_fetchers.api_fetcher import APIFetcher
from pathlib import Path

class Extractor:
    def __init__(self, api_fetcher: APIFetcher):
        self.api_fetcher = api_fetcher

    def extract_to_csv(
            self,
            file_path: Path,
            *,
            api_params: dict[str, Any]
    ) -> None:
        """Extracts data from the API and saves it to a CSV file.

        Args:
            file_path (Path): The path to the CSV file where the extracted data will be saved.
            api_params (dict[str, Any]): A dictionary of parameters to be passed to the API fetcher.
            limit (int): The maximum number of records to be extracted and saved to the CSV file.
            fetch_limit (int): The maximum number of records to be fetched from the API in each request.
        """

        if file_path.suffix != '.csv':
            file_path = file_path.with_suffix('.csv')

