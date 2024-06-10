from scripts.constants import MATCHES_FILE_LOG, FOTMOB_LEAGUE_IDS
from utils.db_connector import connect_to_database
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Union, List, Any
from utils.logger import configure_logger
from utils.link_mapper import LinkMapper
from utils.fetcher import Fetcher
from psycopg2 import sql

# Configure logger for the current module
LOGGER = configure_logger(__name__, MATCHES_FILE_LOG)


class MatchesParser(Fetcher):
    def __init__(self, league: str):
        super().__init__()
        self.league = league
        self.formatted_league = LinkMapper.format_string(league)

    def insert_to_database(self, insert_data: List[List[Union[str, int]]]) -> None:
        """
        Insert processed match data into the database.

        Args:
            - insert_data (List[List[Union[str, int]]]): The data to be inserted into the database.
        """
        with connect_to_database() as connection, connection.cursor() as cursor:
            try:
                query = sql.SQL(
                    """
                    INSERT INTO {}.matches VALUES (
                        %s, %s, %s, %s
                    ) ON CONFLICT DO NOTHING;
                    """
                ).format(sql.Identifier(self.formatted_league))

                cursor.executemany(query, insert_data)
                connection.commit()

            except Exception as e:
                connection.rollback()
                LOGGER.warning(f'Error during the database operation: {e}.')

    @staticmethod
    def _process_match(match: dict, season: str) -> Optional[List[Union[str, int]]]:
        """
        Process a single match to extract relevant data.

        Args:
            match (dict): The match data.
            season (str): The season for which the match data is being processed.

        Returns:
            Optional[List[Union[str, int]]]: A list containing match ID, season, home team name,
            and away team name, or None if the match is not finished.
        """
        if match['status']['finished']:
            return [
                match['id'],
                int(season[:4]),
                match['home']['name'],
                match['away']['name']
            ]
        return None
    
    def get_matches(self, id_league: str, season: str) -> Optional[List[Any]]:
        """
        Fetch and process matches for a given league ID and season.

        Args:
            - id_league (str): The ID of the league.
            - season (str): The season for which to fetch and process matches.

        Returns:
            - Optional[List[Any]]: A list of processed match data or None if an error occurred.
        """
        try:
            fotmob_matches_url = f'https://www.fotmob.com/api/leagues?id={id_league}&season={season}'
            json_content = self.fetch_data(fotmob_matches_url, 'json')
        except Exception as e:
            LOGGER.warning(f'Failed to fetch data: {e}.')
            return None
        
        if not json_content:
            LOGGER.warning(f'Failed to retrieve the element code from the provided link "{fotmob_matches_url}".')
            return None

        matches = json_content.get('matches', {}).get('allMatches', [])
        
        with ThreadPoolExecutor() as executor:
            matches = list(executor.map(lambda match: self._process_match(match, season), matches))
        
        return [match for match in matches if match is not None]

    def start_parse(self) -> None:
        """
        Parse and extract match data for the specified league and insert it into the database.
        """
        # Format the string to a database schema name compliant format
        schema_name = self.formatted_league

        # Check if the schema exists in the database
        with connect_to_database() as connection, connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;"),
                [schema_name]
            )
            if cursor.fetchone() is None:
                LOGGER.warning(f'Schema "{schema_name}" does not exist in the database. Schema initialization is required.')
                return None

        try:
            # Get the league ID from the constants
            id_league = FOTMOB_LEAGUE_IDS[self.league][0]
        except (KeyError, TypeError):
            LOGGER.warning(f'The FOTMOB source does not have data for the specified league "{self.league}".')
            # Find the new source
            return None

        try:
            fotmob_league_url = f'https://www.fotmob.com/api/leagues?id={id_league}'
            json_content = self.fetch_data(fotmob_league_url, 'json')
        except Exception as e:
            LOGGER.warning(f'Failed to fetch data: {e}.')
            return None
        
        if not json_content:
            LOGGER.warning(f'Failed to retrieve the element code from the provided link "{fotmob_league_url}".')
            return None
        
        available_seasons = json_content.get('allAvailableSeasons', [])
        matches_season = []
        for season in available_seasons:
            matches_season.extend(self.get_matches(id_league, season))
        
        if matches_season:
            self.insert_to_database(matches_season)

            LOGGER.info(f'The data has been successfully added to the "{self.league}" '
                        f'schema and the "matches" table.')

def main(league: str) -> None:
    standings_parser = MatchesParser(league)
    standings_parser.start_parse()
