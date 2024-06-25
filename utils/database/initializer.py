from typing import Optional, Tuple
import os

from utils.constants import (DATABASE_INFO_FILE_LOG, HASHMAP_LEAGUE_IDS,
                             DATABASE_FIRST_TABLES, DATABASE_SECOND_TABLES)
from utils.database.connector import connect_to_database
from scripts.eafc.season import define_season
from utils.logger import configure_logger
from utils.link_mapper import LinkMapper

# Configure logger for the current module
LOGGER = configure_logger(os.path.basename(__file__), DATABASE_INFO_FILE_LOG)


def queries(name_schema: str, name_table: str) -> str:
    query = {
        # FOTMOB, TRANSFERMARKT DATA
        'teams': f"""
            CREATE TABLE {name_schema}.teams (
                id INT PRIMARY KEY,
                title VARCHAR(35) NOT NULL
            );
        """,
        'matches': f"""
            CREATE TABLE {name_schema}.matches (
                match_id INT PRIMARY KEY,
                season INT NOT NULL,
                home_id INT NOT NULL REFERENCES {name_schema}.teams (id),
                away_id INT NOT NULL REFERENCES {name_schema}.teams (id)
            );
        """,
        'match_lineups': f"""
            CREATE TABLE {name_schema}.match_lineups (
                match_id INT PRIMARY KEY REFERENCES {name_schema}.matches (match_id),
                lineup_ht VARCHAR(10),
                lineup_at VARCHAR(10)
            );
        """,
        'match_results': f"""
            CREATE TABLE {name_schema}.match_results (
                match_id INT PRIMARY KEY REFERENCES {name_schema}.matches (match_id),
                score_ht INT NOT NULL,
                score_at INT NOT NULL
            );
        """,
        'stadiums': f"""
            CREATE TABLE {name_schema}.stadiums (
                stadium TEXT PRIMARY KEY,
                capacity INT,
                built INT,
                surface VARCHAR(20),
                field_length NUMERIC,
                field_width NUMERIC
            );
        """,
        'match_details': f"""
            CREATE TABLE {name_schema}.match_details (
                match_id INT PRIMARY KEY REFERENCES {name_schema}.matches (match_id),
                utc_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                stadium TEXT DEFAULT 'Undefined' REFERENCES {name_schema}.stadiums (stadium),
                attendance INT,
                reason VARCHAR(25)
            );
        """,
        'players': f"""
            CREATE TABLE {name_schema}.players (
                player_id INT PRIMARY KEY,
                name_player TEXT NOT NULL,
                date_of_birth DATE,
                nationality TEXT,
                height NUMERIC,
                foot TEXT
            );
        """,
        'transfers': f"""
            CREATE TABLE {name_schema}.transfers (
                player_id INT REFERENCES {name_schema}.players (player_id),
                market_value INT,
                transfer_date DATE NOT NULL,
                club TEXT,
                PRIMARY KEY (player_id, transfer_date)
            );
        """,
        # FUTGG DATA (EA FC)
        # Gender - 1: Male, 2: Female
        'cards': f"""
            CREATE TABLE {name_schema}.cards (
                eaid INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                league VARCHAR(80),
                club VARCHAR(80),
                nation VARCHAR(30),
                rarity VARCHAR(50),
                position VARCHAR(5),
                foot VARCHAR(15),
                gender INT,
                CHECK (gender IN (1, 2))
            );
        """,
        'outfield_players': f"""
            CREATE TABLE {name_schema}.outfield_players (
                eaid INT PRIMARY KEY REFERENCES {name_schema}.cards (eaid),
                date_created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                overall INT NOT NULL,
                height INT,
                weak_foot INT NOT NULL,
                skill_moves INT NOT NULL,
                defensive_rate VARCHAR(6) NOT NULL,
                attacking_rate VARCHAR(6) NOT NULL,
                penalties INT NOT NULL,
                pace INT NOT NULL,
                shooting INT NOT NULL,
                passing INT NOT NULL,
                dribbling INT NOT NULL,
                defense INT NOT NULL,
                physical INT NOT NULL,
                sbc BOOLEAN NOT NULL,
                CHECK (defensive_rate IN ('Low', 'Medium', 'High')),
                CHECK (attacking_rate IN ('Low', 'Medium', 'High'))
            );
        """,
        'goalkeepers': f"""
            CREATE TABLE {name_schema}.goalkeepers (
                eaid INT PRIMARY KEY REFERENCES {name_schema}.cards (eaid),
                date_created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                overall INT NOT NULL,
                height INT,
                weak_foot INT NOT NULL,
                skill_moves INT NOT NULL,
                defensive_rate VARCHAR(6) NOT NULL,
                attacking_rate VARCHAR(6) NOT NULL,
                penalties INT NOT NULL,
                diving INT NOT NULL,
                handling INT NOT NULL,
                kicking INT NOT NULL,
                reflexes INT NOT NULL,
                speed INT NOT NULL,
                positioning INT NOT NULL,
                gender
                sbc BOOLEAN NOT NULL,
                CHECK (defensive_rate IN ('Low', 'Medium', 'High')),
                CHECK (attacking_rate IN ('Low', 'Medium', 'High'))
            );
        """,
        'prices': f"""
            CREATE TABLE {name_schema}.prices (
                eaid INT NOT NULL REFERENCES {name_schema}.cards (eaid),
                sold_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                sold_price INT NOT NULL,
                PRIMARY KEY (eaid, sold_date)
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


def create_table(cursor, name_schema: str, name_table: str) -> None:
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


if __name__ == '__main__':
    with connect_to_database() as connection:
        with connection.cursor() as current_cursor:
            leagues = [LinkMapper.format_string(league) for league in HASHMAP_LEAGUE_IDS.keys()]
            
            for league in leagues:
                is_exist_schema = create_schema(current_cursor, league)
                for table_name in DATABASE_FIRST_TABLES:
                    if is_exist_schema:
                        create_table(current_cursor, league, table_name)

            season = define_season(LOGGER)
            if season:
                is_exist_schema = create_schema(current_cursor, f'eafc{season}')
                for table_name in DATABASE_SECOND_TABLES:
                    if is_exist_schema:
                        create_table(current_cursor, f'eafc{season}', table_name)
