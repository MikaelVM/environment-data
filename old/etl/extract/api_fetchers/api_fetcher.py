from abc import ABC, abstractmethod
from typing import Any

import httpx

class APIFetcher(ABC):

    def __init__(self, *, api_timeout: float = 10.0) -> None:
        self.api_timeout = api_timeout

    @property
    @abstractmethod
    def request_url(self) -> str:
        """The URL to which the API request will be sent. This property must be implemented by subclasses."""
        raise NotImplementedError("Request URL property must be implemented by subclasses")

    @property
    @abstractmethod
    def response_columns(self) -> tuple[str, ...]:
        """A tuple of column names that correspond to the data returned by the API. This property must be implemented by subclasses."""
        raise NotImplementedError("Response columns property must be implemented by subclasses")

    def fetch(self, api_parameters: dict = None) -> list[tuple[Any, ...]]:
        """Fetches data from the API and returns it as a list of dictionaries.

        Args:
            api_parameters (dict, optional): A dictionary of parameters to be sent with the API request. Defaults to None.
        """
        json_response = self._get_result_as_json(api_parameters)
        return self._parse_json_response(json_response) if json_response else []

    def _get_result_as_json(self, params: dict[Any, Any] = None) -> dict[str, Any] | None:
        response = httpx.get(self.request_url, params=params, timeout=self.api_timeout)

        if response.status_code != 200:
            print(f"Error: API request to {self.request_url} failed with status code {response.status_code} with params {params} and response content: {response.text}")
            return None

        return response.json() if response and response.status_code == 200 else None

    @abstractmethod
    def _parse_json_response(self, json_response: dict[str, Any]) -> list[tuple[Any, ...]]:
        """ Parses the JSON response from the API and returns it as a list of lists, where each inner list represents a row of data.

        Args:
            json_response (dict[str, Any]): The JSON response from the API to be parsed.
        """
        raise NotImplementedError("Parse JSON response method must be implemented by subclasses")
