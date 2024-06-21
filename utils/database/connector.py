from psycopg2 import connect, extensions, OperationalError
from typing import List, Any
from decouple import config

from utils.constants import DATABASE_INFO_FILE_LOG
from utils.logger import configure_logger

# Configure logger for the current module
LOGGER = configure_logger(__name__, DATABASE_INFO_FILE_LOG)


def connect_to_database() -> extensions.connection:
    """
    Establishes a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: Database connection object if successful.
    """
    try:
        # Attempt to establish a database connection
        with connect(
            database='football_competitions',
            user=config('PG_USER'),
            password=config('PG_PASSWORD'),
            host=config('PG_HOST'),
            port='5432'
        ) as current_connection:
            return current_connection

    except OperationalError as e:
        LOGGER.fatal(f'Error connecting to the database: {str(e).strip()}.')
        raise

def insert_data(connection: extensions.connection, 
                schema_name: str, table_name: str, data: List[List[Any]]) -> None:
    """
    Inserts data into a specified table.

    Args:
        connection (psycopg2.extensions.connection): Database connection object.
        schema_name (str): Name of the schema where the table is located.
        table_name (str): Name of the table where data should be inserted.
        data (List[List[Any]]): List of rows, where each row is a list of values to be inserted.
    """
    try:
        with connection.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(data[0]))
            query = f"""
                INSERT INTO {schema_name}.{table_name} VALUES (
                    {placeholders}
                ) ON CONFLICT DO NOTHING;
                """
            cursor.executemany(query, data)
            connection.commit()

    except Exception as e:
        connection.rollback()
        LOGGER.error(f'Error inserting data into "{schema_name}.{table_name}": {str(e)}.')
        raise
