from old.postgresql_runner import PostgreSQLRunner
from pathlib import Path
from etl.helper_functions import file_to_sql, get_sql_files_from_folder

def init_db(query_runner: PostgreSQLRunner, verbose: bool = True) -> None:
    """Initializes the database by running all SQL scripts in the 'sql' folder."""
    sql_folder = Path(__file__).parent / 'sql'
    sql_files = get_sql_files_from_folder(sql_folder)

    for file in sql_files:
        print(f"Running initialization script: {file.name}") if verbose else None
        sql = file_to_sql(file)
        if query_runner.run_query(sql):
            print(f"Successfully ran {file.name}") if verbose else None
        else:
            print(f"Failed to run {file.name}") if verbose else None