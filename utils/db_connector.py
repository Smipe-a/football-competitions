from psycopg2 import connect, OperationalError
from utils.logger import configure_logger
from decouple import config


def connect_to_database(competition: str):
    """
    Establishes a connection to the PostgreSQL database.

    Args:
        competition (str): The identifier of the football competition for logger.

    Returns:
        psycopg2.extensions.connection: Database connection object if successful.
    """

    # Configure logger for the current module
    logger = configure_logger(__name__, competition)

    try:
        # Attempt to establish a database connection
        with connect(
            database='football_competitions',
            user=config('PG_USER'),
            password=config('PG_PASSWORD'),
            host=config('PG_HOST'),
            port='5432'
        ) as connection:
            logger.info('Successfully connected to the database.')
            return connection

    except OperationalError as e:
        logger.error(f'Error connecting to the database: {str(e)}.')
