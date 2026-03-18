import httpx

class APIFetcher:
    def __init__(self, *, api_timeout: float = 10.0, verbose: bool = False) -> None:
        self.api_timeout = api_timeout
        self.verbose = verbose

    def fetch(self, request_url, api_parameters: dict = None) -> httpx.Response:
        """Fetches data from the API and returns it as a list of dictionaries.

        Args:
            request_url (str): The URL to which the API request will be sent.
            api_parameters (dict, optional): A dictionary of parameters to be sent with the API request. Defaults to None.
        """
        response = httpx.get(request_url, params=api_parameters, timeout=self.api_timeout)

        if response.status_code != 200:
            print(
                f"Error: API request to {request_url} failed."
                f" - Status code {response.status_code}"
                f" - Params: {api_parameters}"
                f" - Response content: {response.text}"
            ) if self.verbose else None

        return response
