from typing import Any, Dict
from datetime import datetime
from dateutil import parser
import  httpx

def dmi_datetime_parser(date_str: str) -> datetime:
    """Parses a date string in the format '%Y-%m-%-dT%-H:%M:%SZ' to a datetime object.

    Args:
        date_str (str): The date string to parse.
    """
    return parser.parse(date_str) if date_str else None