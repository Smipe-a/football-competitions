from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from typing import Optional, Tuple
import os

COMPETITIONS_TITLE = ['premier_league', 'la_liga', 'ligue_1', 'bundesliga']

# Path to file log <your_abspath>/football-competitions/logs/database_info.log
NAME_DATABASE_FILE_LOG = 'database_info'
# Configure logger for the current module
LOGGER = configure_logger(os.path.basename(__file__), NAME_DATABASE_FILE_LOG)


def check_schema(cursor, name_schema: str) -> Optional[Tuple[str]]:
    """
    Check if the schema already exists in the database.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name to check.

    Returns:
        Tuple[str]: The schema name if it exists, otherwise None.
    """
    cursor.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;', (name_schema,))
    return cursor.fetchone()


def create_schema(cursor, name_schema: str) -> Optional[bool]:
    """
    Create a new schema in the database if it does not exist.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name to create.

    Returns:
        bool: True if the schema is created or already exists, False on error.
    """
    try:
        existing_schema = check_schema(cursor, name_schema)

        if not existing_schema:
            cursor.execute(f'CREATE SCHEMA {name_schema};')
            connection.commit()
            LOGGER.info(f'Schema "{name_schema}" has been successfully created in the database.')
        else:
            LOGGER.info(f'The schema "{name_schema}" already exist.')

        return True
    except Exception as e:
        LOGGER.error(f'Error creating schema: {e}')
        return None


def create_table(cursor, name_schema: str) -> None:
    """
    Create a standings table in the specified schema.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name in which to create the table.
    """
    try:
        # Check if a table with this season already exists
        check_table_query = """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                            """
        cursor.execute(check_table_query, (name_schema, 'standings'))

        if cursor.fetchone() is None:
            init_standings = {
                'standings': f"""
                    CREATE TABLE IF NOT EXISTS {name_schema}.standings (
                        team VARCHAR(35),
                        season VARCHAR(5),
                        position INT NOT NULL,
                        won INT NOT NULL,
                        drawn INT NOT NULL,
                        lost INT NOT NULL,
                        goals_for INT NOT NULL,
                        goals_against INT NOT NULL,
                        points INT NOT NULL,
                        PRIMARY KEY (team, season)
                    );
                """
            }

            create_table_query = (
                init_standings['standings']
            )
            cursor.execute(create_table_query)
            connection.commit()

            LOGGER.info(f'The table "standings" has been successfully created in the schema "{name_schema}".')
        else:
            LOGGER.info(f'The table "standings" in the schema "{name_schema}" already exist.')

        cursor.close()
    except Exception as e:
        LOGGER.error(f'Error creating table: {e}')


if __name__ == '__main__':
    connection = connect_to_database()

    for competition in COMPETITIONS_TITLE:
        with connection.cursor() as current_cursor:
            is_exist_schema = create_schema(cursor=current_cursor, name_schema=competition)

            if is_exist_schema:
                create_table(cursor=current_cursor, name_schema=competition)

    connection.close()
