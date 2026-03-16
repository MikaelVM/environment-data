from pathlib import Path
from configparser import ConfigParser

import httpx

def get_auth_token(config_path: Path, config_section: str) -> httpx.BasicAuth:
        """Fetches the authentication token from the configuration file."""
        config = ConfigParser()
        config.read(config_path)

        if config_section not in config:
            raise ValueError(f"Config section '{config_section}' not found in {config_path}")


        return httpx.BasicAuth(
            username=config[config_section]['username'],
            password=config[config_section]['password']
        )

# TODO: Might be better to use auth token in Client object - See https://docs.authlib.org/en/latest/client/httpx.html
