from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Union, List, Set, Tuple
from psycopg2 import extensions

from utils.database.connector import connect_to_database, insert_data
from utils.constants import MATCHES_FILE_LOG, HASHMAP_LEAGUE_IDS
from utils.link_mapper import format_string
from utils.logger import configure_logger
from utils.fetcher import Fetcher

# Configure logger for the current module
LOGGER = configure_logger(__name__, MATCHES_FILE_LOG)


class FotmobMatches(Fetcher):
    """
    A class used to fetch match data from FOTMOB for a specific league.

    Args:
        league (str): The name of the league for which match data will be fetched.

    Attributes:
        url (str): The base URL for the FOTMOB API.
        schema_name (str): The name of the database schema where the data will be stored.
        finished_matches (int): The number of matches successfully processed and inserted.
        total_matches (int): The total number of matches fetched for processing.
    """
    def __init__(self, league: str):
        super().__init__()
        self.league = league

        self.url = 'https://www.fotmob.com/api/'
        self.schema_name = format_string(league)
        self.finished_matches = 0
        self.total_matches = 0

    @staticmethod
    def _process_match(
        match: dict, season: str,
        teams: Set[Tuple[int, str]]) -> Optional[List[Union[int, str]]]:
        """
        Process a single match to extract relevant data.

        Args:
            match (dict): The match data.
            season (str): The season for which the match data is being processed.
            teams (Set[Tuple[int, str]]): 

        Returns:
            A list containing match ID, season, home team name,
            and away team name, or None if the match is not finished.
        """
        if match.get('status', {}).get('finished', False):
            teams.add((match['home']['id'], match['home']['name']))
            
            return [match['id'], int(season[:4]), match['home']['id'], match['away']['id']]
        
        return None
    
    def get_matches(self, connection: extensions.connection,
                    id_league: Optional[str], season: str) -> None:
        """
        Fetch and process matches for a given league ID and season.

        Args:
            connection (extensions.connection): Database connection object.
            id_league (str): The ID of the league.
            season (str): The season for which to fetch and process matches.
        """
        try:
            matches_url = self.url + f'leagues?id={id_league}&season={season}'
            json_content = self.fetch_data(matches_url, 'json')

            if not json_content:
                LOGGER.warning(f'Failed to retrieve the json from the provided link "{matches_url}".')
                return None
        except Exception as e:
            LOGGER.warning(f'Failed to fetch data: {e}.')
            return None
        
        matches = json_content.get('matches', {}).get('allMatches', [])
        # Counting the total amount of data across all seasons, including both finished and unfinished
        self.total_matches += len(matches)

        # Collecting all possible unique pairs (team id, team title) into a set
        teams = set()

        with ThreadPoolExecutor() as executor:
            season_matches = executor.map(lambda match: self._process_match(match, season, teams), matches)
        
        # Initially adding command keys to the database, ensuring no foreign key exceptions occur
        season_teams = [[team[0], team[1]] for team in teams if team is not None]
        if season_teams:
            try:
                insert_data(connection, self.schema_name, 'teams', season_teams)
            except Exception:
                LOGGER.error(f'League data "{self.schema_name}" for the season {season_teams} was not inserted.')
                return None

        # Filter out None values from the list to prevent exceptions in insert_data
        season_matches = [match for match in list(season_matches) if match is not None]
        if season_matches:
            try:
                # Data insertion via 'connector'
                insert_data(connection, self.schema_name, 'matches', season_matches)
                # Increasing the amount of data inserted into the database,
                # regardless of whether there was a key conflict or not
                self.finished_matches += len(season_matches)
            except Exception:
                LOGGER.error(f'League data "{self.schema_name}" for the season {season_teams} was not inserted.')
                return None

    def start_parse(self) -> None:
        """
        Parse and extract match data for the specified league and insert it into the database.
        """
        try:
            # Get the league ID from the utils.constants.HASHMAP_LEAGUE_IDS
            id_league = HASHMAP_LEAGUE_IDS[self.league][0]
        except (KeyError, TypeError):
            LOGGER.warning(f'The FOTMOB source does not have data for the specified league "{self.league}".')
            
            # Find the new source (female competitions)
            return None

        try:
            league_url = self.url + f'leagues?id={id_league}'
            json_content = self.fetch_data(league_url, 'json')

            if not json_content:
                LOGGER.warning(f'Failed to retrieve the json from the provided link "{league_url}".')
                return None
        except Exception as e:
            LOGGER.warning(f'Failed to fetch data: {e}.')
            return None
        
        # To speed up the data collection process,
        # only the current season (available_seasons[:1]) is considered
        # If data from previous seasons is needed,
        # a slice from the second season onward [1:] should be used
        with connect_to_database() as connection:
            available_seasons = json_content.get('allAvailableSeasons', [])
            for season in available_seasons[:1]:
                self.get_matches(connection, id_league, season)
        
        LOGGER.info(f'Successfully inserted {self.finished_matches} out of {self.total_matches} ' \
                    f'possible data entries into the ' \
                    f'"{self.schema_name}.matches" table (finished matches).')


def main(league: str) -> None:
    fotmob_matches = FotmobMatches(league)
    fotmob_matches.start_parse()
