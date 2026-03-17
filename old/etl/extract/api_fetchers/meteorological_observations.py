from typing import Any, Dict

from .api_fetcher import APIFetcher
from .helper_functions import dmi_datetime_parser
import  httpx

class MeteorologicalObservationsFetcher(APIFetcher):

    def __init__(self, *, api_timeout: float = 10.0) -> None:
        super().__init__(api_timeout=api_timeout)

    @property
    def request_url(self) -> str:
        return "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"

    @property
    def response_columns(self) -> tuple[str, ...]:
        return (
            'observation_id',
            'longitude',
            'latitude',
            'parameter_id',
            'created',
            'value',
            'observed',
            'station_id'
        )

    def _parse_json_response(self, json_response: dict[str, Any]) -> list[tuple[Any, ...]]:
        result = []
        if json_response:
            for observation in json_response['features']:
                result.append(
                    (
                        observation['id'],
                        observation['geometry']['coordinates'][0],
                        observation['geometry']['coordinates'][1],
                        observation['properties']['parameterId'],
                        observation['properties']['created'],
                        observation['properties']['value'],
                        observation['properties']['observed'],
                        observation['properties']['stationId'],
                    )
                )
        return result
