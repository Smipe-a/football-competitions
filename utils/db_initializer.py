from scripts.constants import DATABASE_INFO_FILE_LOG, LEAGUES, DATABASE_TABLES
from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from typing import Optional, Tuple
import os

# Configure logger for the current module
LOGGER = configure_logger(os.path.basename(__file__), DATABASE_INFO_FILE_LOG)


def queries(name_schema: str, name_table: str) -> str:
    query = {
        'standings': f"""
            CREATE TABLE {name_schema}.standings (
                team VARCHAR(35) NOT NULL,
                season INT NOT NULL,
                position INT NOT NULL,
                won INT NOT NULL,
                drawn INT NOT NULL,
                lost INT NOT NULL,
                goals_for INT NOT NULL,
                goals_against INT NOT NULL,
                points INT NOT NULL,
                PRIMARY KEY (team, season)
            );
        """,
        'clashes': f"""
            CREATE TABLE {name_schema}.clashes (
                match_id INT PRIMARY KEY,
                season INT NOT NULL,
                name_ht VARCHAR(35) NOT NULL,
                name_at VARCHAR(35) NOT NULL,
                FOREIGN KEY (name_ht, season) REFERENCES {name_schema}.standings (team, season),
                FOREIGN KEY (name_at, season) REFERENCES {name_schema}.standings (team, season)
            );
        """,
        'lineups': f"""
            CREATE TABLE {name_schema}.lineups (
                match_id INT PRIMARY KEY REFERENCES {name_schema}.clashes (match_id),
                lineup_ht VARCHAR(10),
                lineup_at VARCHAR(10)
            );
        """,
        'result_clashes': f"""
            CREATE TABLE {name_schema}.result_clashes (
                match_id INT PRIMARY KEY REFERENCES {name_schema}.clashes (match_id),
                score_ht INT NOT NULL,
                score_at INT NOT NULL
            );
        """,
        'stadiums': f"""
            CREATE TABLE {name_schema}.stadiums (
                stadium VARCHAR(25) PRIMARY KEY,
                owner TEXT,
                address TEXT UNIQUE,
                capacity INT,
                built INT,
                surface VARCHAR(20),
                field_length NUMERIC,
                field_width NUMERIC
            );
        """,
        'info_clashes': f"""
            CREATE TABLE {name_schema}.info_clashes (
                match_id INT PRIMARY KEY REFERENCES {name_schema}.clashes (match_id),
                utc_time TIMESTAMP NOT NULL,
                stadium TEXT DEFAULT NULL REFERENCES {name_schema}.stadiums (stadium),
                attendance INT DEFAULT NULL,
                reason VARCHAR(5)
            );
        """,
        'addresses': f"""
            CREATE TABLE {name_schema}.addresses (
                address TEXT PRIMARY KEY REFERENCES {name_schema}.stadiums (address),
                longitude NUMERIC NOT NULL,
                latitude NUMERIC NOT NULL
            );
        """
    }

    return query[name_table]


def check_schema(cursor, name_schema: str) -> Optional[Tuple[str]]:
    """
    Check if the schema already exists in the database.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name to check.
    """
    cursor.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;', (name_schema,))
    return cursor.fetchone()


def create_schema(cursor, name_schema: str) -> Optional[bool]:
    """
    Create a new schema in the database if it does not exist.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name to create.
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
        LOGGER.error(f'Error creating schema: {str(e).strip()}.')
        return None


def create_table(cursor, name_schema: str) -> None:
    """
    Create a standings table in the specified schema.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name in which to create the table.
    """
    try:
        for name_table in DATABASE_TABLES:
            # Check if a table with this season already exists
            check_table_query = """
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = %s AND table_name = %s
                                """
            cursor.execute(check_table_query, (name_schema, name_table))

            # If such a table is absent, we create it
            if cursor.fetchone() is None:
                cursor.execute(queries(name_schema, name_table))
                connection.commit()

                LOGGER.info(f'The table "{name_table}" has been successfully created in the schema "{name_schema}".')
            else:
                LOGGER.info(f'The table "{name_table}" in the schema "{name_schema}" already exist.')

    except Exception as e:
        LOGGER.error(f'Error creating table: {str(e).strip()}.')
    finally:
        cursor.close()


if __name__ == '__main__':
    with connect_to_database() as connection:
        for competition in LEAGUES:
            with connection.cursor() as current_cursor:
                is_exist_schema = create_schema(current_cursor, competition)

                if is_exist_schema:
                    create_table(current_cursor, competition)
