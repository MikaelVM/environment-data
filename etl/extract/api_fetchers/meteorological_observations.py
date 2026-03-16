from typing import Any, Dict
from .helper_functions import dmi_datetime_parser

import  httpx

def fetch_meteorological_observations(
        *,
        datetime_search: str = None,
        station_id: str = None,
        limit: int = None,
        offset: int = None,
        sort_order: str = 'observed,DESC'
) -> list[Dict[str, Any]]:
    """Fetches meteorological observation data from the DMI API.

    Args:
        datetime_search (str, optional): A date string in the format '%Y-%m-%-dT%-H:%M:%SZ' to filter observations by datetime.
            If None, no filtering will be applied. Defaults to None.
        station_id (str, optional): The ID of the station to fetch observations for.
            If None, data for all stations will be fetched. Defaults to None.
        limit (int, optional): The maximum number of observation records to return.
            If None, all records will be returned. Defaults to None.
        offset (int, optional): The number of records to skip before starting to return records.
            If None, no records will be skipped. Defaults to None.
    """

    request_url = f"https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
    params = {}

    if datetime_search:
        params['datetime'] = datetime_search
    if station_id:
        params['stationId'] = station_id
    if limit is not None:
        params['limit'] = limit
    if offset is not None:
        params['offset'] = offset
    if sort_order:
        params['sortorder'] = sort_order

    response = httpx.get(request_url, params=params, timeout=10.0)
    data = response.json() if response and response.status_code == 200 else None

    result = []
    if data:
        for observation in data['features']:
            result.append(
                {
                    'id': observation['id'],
                    'longitude': observation['geometry']['coordinates'][0],
                    'latitude': observation['geometry']['coordinates'][1],
                    'parameter_id': observation['properties']['parameterId'],
                    'created': dmi_datetime_parser(observation['properties']['created']),
                    'value': observation['properties']['value'],
                    'observed': dmi_datetime_parser(observation['properties']['observed']),
                    'station_id': observation['properties']['stationId'],
                }
            )

    return result