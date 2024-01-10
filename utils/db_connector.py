from psycopg2 import connect, OperationalError
from utils.logger import configure_logger
from decouple import config


# Configure logger for the current module
LOGGER = configure_logger(__name__)


def connect_to_database():
    """
    Establishes a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: Database connection object if successful, None otherwise.
    """

    try:
        # Attempt to establish a database connection
        connection = connect(
            database='football_competitions',
            user=config('PG_USER'),
            password=config('PG_PASSWORD'),
            host=config('PG_HOST'),
            port='5432'
        )

        LOGGER.info('Successfully connected to the database.')
        return connection

    except OperationalError as e:
        LOGGER.error(f'Error connecting to the database: {str(e)}.')
        return None
