from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from typing import Optional
import os

COMPETITIONS_TITLE = ['premier_league', 'la_liga', 'serie_a', 'bundesliga', 'ligue_1']
DATABASE_TABLE_NAME = ['standings', 'team_clashes', 'result_clashes', 'info_clashes',
                       'home_match_statistics', 'away_match_statistics']

# Path to file log <your_abspath>/football-competitions/logs/database_info.log
NAME_DATABASE_FILE_LOG = 'database_info.log'
# Configure logger for the current module
LOGGER = configure_logger(os.path.basename(__file__), NAME_DATABASE_FILE_LOG)


def check_schema(cursor, name_schema: str) -> Optional[tuple[str]]:
    """
    Check if the schema already exists in the database.

    Args:
        cursor: Database cursor.
        name_schema (str): Schema name to check.

    Returns:
        tuple[str]: The schema name if it exists, otherwise None.
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
        Optional[bool]: True if the schema is created or already exists, False on error.
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
        for table_name in DATABASE_TABLE_NAME:
            # Check if a table with this season already exists
            check_table_query = """
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = %s AND table_name = %s
                                """
            cursor.execute(check_table_query, (name_schema, table_name))

            if cursor.fetchone() is None:
                init_table = {
                    'standings': f"""
                        CREATE TABLE IF NOT EXISTS {name_schema}.standings (
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
                    'team_clashes': f"""
                        CREATE TABLE IF NOT EXISTS {name_schema}.team_clashes (
                            match_id INT PRIMARY KEY,
                            season INT NOT NULL,
                            home_team VARCHAR(35) NOT NULL,
                            away_team VARCHAR(35) NOT NULL,
                            FOREIGN KEY (home_team, season) REFERENCES {name_schema}.standings (team, season),
                            FOREIGN KEY (away_team, season) REFERENCES {name_schema}.standings (team, season)
                        );
                    """,
                    'result_clashes': f"""
                        CREATE TABLE IF NOT EXISTS {name_schema}.result_clashes (
                            match_id INT PRIMARY KEY REFERENCES {name_schema}.team_clashes (match_id),
                            score_ht INT NOT NULL,
                            score_at INT NOT NULL
                        );
                    """,
                    'info_clashes': f"""
                        CREATE TABLE IF NOT EXISTS {name_schema}.info_clashes (
                            match_id INT PRIMARY KEY REFERENCES {name_schema}.team_clashes (match_id),
                            date_start TIMESTAMP NOT NULL,
                            stadium TEXT DEFAULT NULL,
                            attendance INT DEFAULT NULL
                        );
                    """,
                    'home_match_statistics': f"""
                        CREATE TABLE IF NOT EXISTS {name_schema}.home_match_statistics (
                            match_id INT PRIMARY KEY REFERENCES {name_schema}.team_clashes (match_id),
                            possessions NUMERIC,
                            on_target INT,
                            off_target INT,
                            blocked INT,
                            passing NUMERIC,
                            clear_cut_chance INT,
                            corners INT,
                            offsides INT,
                            tackles NUMERIC,
                            aerial_duels NUMERIC,
                            saves INT,
                            fouls_committed INT,
                            fouls_won INT
                        );
                    """,
                    'away_match_statistics': f"""
                        CREATE TABLE IF NOT EXISTS {name_schema}.away_match_statistics (
                            match_id INT PRIMARY KEY REFERENCES {name_schema}.team_clashes (match_id),
                            possessions NUMERIC,
                            on_target INT,
                            off_target INT,
                            blocked INT,
                            passing NUMERIC,
                            clear_cut_chance INT,
                            corners INT,
                            offsides INT,
                            tackles NUMERIC,
                            aerial_duels NUMERIC,
                            saves INT,
                            fouls_committed INT,
                            fouls_won INT
                        );
                    """
                }

                create_table_query = (init_table[table_name])
                cursor.execute(create_table_query)
                connection.commit()

                LOGGER.info(f'The table "{table_name}" has been successfully created in the schema "{name_schema}".')
            else:
                LOGGER.info(f'The table "{table_name}" in the schema "{name_schema}" already exist.')

    except Exception as e:
        LOGGER.error(f'Error creating table: {str(e).strip()}.')
    finally:
        cursor.close()


if __name__ == '__main__':
    with connect_to_database() as connection:
        for competition in COMPETITIONS_TITLE:
            with connection.cursor() as current_cursor:
                is_exist_schema = create_schema(cursor=current_cursor, name_schema=competition)

                if is_exist_schema:
                    create_table(cursor=current_cursor, name_schema=competition)
