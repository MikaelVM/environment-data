from etl.gatherer import get_met_station
import json

if __name__ == "__main__":
    response = get_met_station()
    stations = response.json()
    longitude, latitude = stations['features'][0]['geometry']['coordinates']
    print(f"Longitude {longitude}, Latitude: {latitude}")

    data = response.json() if response and response.status_code == 200 else None

    if data:
        for station in data['features']:
            print(f"Name: {station['properties']['name']} - StationID: {station['properties']['stationId']}")

