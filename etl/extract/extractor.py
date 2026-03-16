import csv
from typing import Any

from .api_fetchers.api_fetcher import APIFetcher
from pathlib import Path

class Extractor:
    def __init__(self, api_fetcher: APIFetcher):
        self.api_fetcher = api_fetcher

    def extract_to_csv(self, file_path: Path, *, api_params: dict[str, Any], limit: int = None, fetch_limit: int = None) -> None:
        """Extracts data from the API and saves it to a CSV file.

        Args:
            file_path (Path): The path to the CSV file where the extracted data will be saved.
            api_params (dict[str, Any]): A dictionary of parameters to be passed to the API fetcher.
            limit (int): The maximum number of records to be extracted and saved to the CSV file.
            fetch_limit (int): The maximum number of records to be fetched from the API in each request.
        """
        offset = 0
        total_fetched = 0

        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self.api_fetcher.response_columns)

            while True:
                if fetch_limit is not None and total_fetched >= fetch_limit:
                    break

                api_params_with_pagination = {**api_params, 'limit': fetch_limit, 'offset': offset}
                data = self.api_fetcher.fetch(api_params_with_pagination)

                if not data:
                    break

                for record in data:
                    writer.writerow(record)

                total_fetched += len(data)
                offset += len(data)

                if limit is not None and total_fetched >= limit:
                    break


