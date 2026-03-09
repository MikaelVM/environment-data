import  httpx

def get_met_station(*, station_id :str = None, limit: int = None, offset: int = None ) -> httpx.Response:

    request_url = f"https://opendataapi.dmi.dk/v2/metObs/collections/station/items?"
    if station_id:
        request_url += f"stationId={station_id}"

    return httpx.get(request_url)