from psycopg2 import connect, extensions, OperationalError
from scripts.constants import DATABASE_INFO_FILE_LOG
from utils.logger import configure_logger
from decouple import config

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
            LOGGER.info('Successfully connected to the database.')
            return current_connection

    except OperationalError as e:
        LOGGER.fatal(f'Error connecting to the database: {str(e).strip()}.')
        raise
