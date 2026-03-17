from typing import Any, Dict
from datetime import datetime
from .helper_functions import dmi_datetime_parser
import  httpx

def fetch_stations(
        *,
        station_id :str = None,
        limit: int = None,
        offset: int = None
) -> list[Dict[str, Any]]:
    """Fetches meteorological station data from the DMI API.

    Args:
        station_id (str, optional): The ID of the station to fetch.
            If None, data for all stations will be fetched. Defaults to None.
        limit (int, optional): The maximum number of station records to return.
            If None, all records will be returned. Defaults to None.
        offset (int, optional): The number of records to skip before starting to return records.
            If None, no records will be skipped. Defaults to None.
    """

    request_url = f"https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
    params: dict = {}

    if station_id:
        params['stationId'] = station_id
    if limit is not None:
        params['limit'] = limit
    if offset is not None:
        params['offset'] = offset

    response = httpx.get(request_url, params=params, timeout=10.0)
    data = response.json() if response and response.status_code == 200 else None

    result = []
    if data:
        for station in data['features']:
            result.append(
                {
                    'name': station['properties']['name'],
                    'owner': station['properties']['owner'],
                    'country': station['properties']['country'],
                    'longitude': station['geometry']['coordinates'][0],
                    'latitude': station['geometry']['coordinates'][1],
                    'created': dmi_datetime_parser(station['properties']['created']),
                    'operation_from': dmi_datetime_parser(station['properties']['operationFrom']),
                    'operation_to': dmi_datetime_parser(station['properties']['operationTo']),
                    'valid_from': dmi_datetime_parser(station['properties']['validFrom']),
                    'valid_to': dmi_datetime_parser(station['properties']['validTo']),
                    'updated': dmi_datetime_parser(station['properties']['updated']),
                    'parameter_id': station['properties']['parameterId'],
                    'anemometer_height': station['properties']['anemometerHeight'],
                    'barometer_height': station['properties']['barometerHeight'],
                    'station_height': station['properties']['stationHeight']
                }
            )

    return result
