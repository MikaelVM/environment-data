# Request URL for DMI Meteorological Observations API
dmi_mo_api_request_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'
fetch_limit = 1000
api_params = {
    'stationId': '06180'
}
file_name = 'meteorological_observations_data.csv'

fetch_in_batches(
    api_fetcher,
    api_params,
    csv_folder,
    dmi_mo_api_request_url,
    fetch_limit,
    file_name)


def fetch_in_batches(api_fetcher: APIFetcher, api_params: dict[str, str], csv_folder: Path, dmi_mo_api_request_url: str,
                     fetch_limit: int, file_name: str):
    # Loop to fetch data in batches and save to CSV file until no more data is returned
    offset = 0
    while True:
        mo_json_response = api_fetcher.fetch(
            dmi_mo_api_request_url,
            api_parameters={**api_params, 'limit': fetch_limit, 'offset': offset}).json()

        mo_data = pd.json_normalize(mo_json_response['features'])
        if mo_data.empty:
            break
        mo_data.to_csv(csv_folder / file_name, mode='a', index=False,
                       header=not (csv_folder / 'meteorological_observations_data.csv').exists())
        offset += fetch_limit

# --------------

    start_date = datetime(2025, 1, 1)
    station_id = '06180'
    api_params = {
        'stationId': station_id,
        'limit': 100000,
    }
    offset = api_params['limit']

    for month in range(1, 13):
        print_centered_message(f' Fetching data for month {month} of 2025 ', fillchar='-')
        end_day = calendar.monthrange(2025, month)[1]
        end_date = datetime(2025, month, end_day)
        api_params = {
            **api_params,
            'datetime': construct_datetime_argument(start_date, end_date),
        }

        if 'offset' in api_params:
            del api_params['offset']

        print(f'Api parameters for iteration 0: {api_params}')
        data = api_fetcher.fetch(dmi_mo_api_request_url, api_parameters=api_params).json()
        iteration = 1

        while data['numberReturned'] > 0:
            api_params = {
                **api_params,
                'offset': iteration * offset,
            }
            print(f'Api parameters for iteration {iteration}: {api_params}')

            json_handler.write_json(
                file_path=observation_folder / '2025' / f'station_{station_id}_m{month}_p{iteration}.json',
                data=data
            )

            iteration += 1
            data = api_fetcher.fetch(dmi_mo_api_request_url, api_parameters=api_params).json()
