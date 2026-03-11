from pathlib import Path
from typing import LiteralString, Literal

from psycopg.sql import SQL

def add_parameter_to_request_url(request_url: str, parameter_name: str, parameter_value: str) -> str:
    """Adds a query parameter to the request URL.

    Args:
        request_url (str): The base request URL.
        parameter_name (str): The name of the query parameter to add.
        parameter_value (str): The value of the query parameter to add.

    Returns:
        str: The updated request URL with the new query parameter.
    """
    if not request_url.endswith("&") and not request_url.endswith("?"):
        request_url += "&"
    request_url += f"{parameter_name}={parameter_value}"
    return request_url

def file_to_sql(file_path: Path) -> SQL:
    """Reads the contents of a SQL file and returns it as a psycopg SQL object.

    Args:
        file_path (Path): The path to the SQL file to read.

    Exceptions:
        FileNotFoundError: If the specified file does not exist or is not a file.
        ValueError: If the specified file is not a .sql file or if the file is empty.
    """
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.suffix.lower() != '.sql':
        raise ValueError(f"Invalid file type: {file_path.suffix}. Expected a .sql file.")

    if file_path.stat().st_size == 0:
        raise ValueError(f"File is empty: {file_path}")

    with open(file_path) as file:
        # TODO: This is a bit hacky, but it seems that psycopg's SQL class does not accept a regular string as input.
        file_string = file.read()

    return SQL(file_string)

def get_sql_files_from_folder(folder_path: Path) -> list[Path]:
    """Returns a list of all .sql files in the specified folder.

    Args:
        folder_path (Path): The path to the folder to search for .sql files.

    Exceptions:
        FileNotFoundError: If the specified folder does not exist, is not a directory or
            if no .sql files are found in the folder.
    """
    if not folder_path.exists() or not folder_path.is_dir():
        raise FileNotFoundError(f"SQL folder '{folder_path}' does not exist or is not a directory.")

    if not any(folder_path.glob('*.sql')):
        raise FileNotFoundError(f"No .sql files found in '{folder_path}'.")

    return list(folder_path.glob('*.sql'))