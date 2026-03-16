"""Module for running SQL queries on a PostgreSQL database."""
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from psycopg import Connection, connect
from psycopg.sql import SQL

class PostgreSQLRunner:
    """A class for running SQL queries on a PostgreSQL database."""

    def __init__(self, config_file_path: Path, *, verbose: bool = True) -> None:
        self.verbose = verbose
        self.connection_string = self._get_connection_string_from_config(config_file_path)
        print("PostgreSQLRunner initialized successfully.") if self.verbose else None

    def run_query(self, query: SQL, values: list[Any] = None) -> bool:
        """Executes a SQL query on the PostgreSQL database.

        Args:
            query (SQL): The SQL query to be executed.
            values (list[Any], optional): A list of values to be passed to the query. Defaults to None.

        Returns:
            bool: True if the query was executed successfully, False otherwise.
        """
        connection = self._get_postgres_connection()
        try:
            print(f"Running query: \n" + '-' * 50 +
                  f"\n{query.as_string(connection)}\n") if self.verbose else None
            print('-' * 50) if self.verbose else None

            with connection as conn:
                if values:
                    conn.execute(query, values)
                else:
                    conn.execute(query)

            return True

        except Exception as e:
            print(f"Error executing query: {e}")
            return False

    def bulk_insert(self, query: SQL, values: list[list[Any]]) -> bool:
        connection = self._get_postgres_connection()
        pass


    def _get_postgres_connection(self) -> Connection:
        """Establishes a connection to the PostgreSQL database using the connection string."""

        try:
            connection = connect(self.connection_string)
            print("Connection to PostgreSQL database established successfully.") if self.verbose else None
            return connection
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    @staticmethod
    def _get_connection_string_from_config(config_file_path: Path) -> str:
        """Reads database connection parameters from a configuration file and constructs a connection string.

        Args:
            config_file_path (Path): The path to the configuration file containing the database connection parameters.
        """
        config = ConfigParser()
        config.read(config_file_path)

        host = config['DBCONFIG']['host']
        port = config['DBCONFIG']['port']
        dbname = config['DBCONFIG']['dbname']
        user = config['DBCONFIG']['user']
        password = config['DBCONFIG']['password']

        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        return connection_string