from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Union, Tuple, List
from psycopg2 import extensions
from datetime import datetime

from utils.database.connector import connect_to_database, insert_data
from utils.constants import STADIUMS_FILE_LOG
from utils.link_mapper import format_string
from utils.logger import configure_logger
from utils.fetcher import Fetcher

# Configure logger for the current module
LOGGER = configure_logger(__name__, STADIUMS_FILE_LOG)


class FotmobStadiums(Fetcher):
    """
    A class to fetch and store stadium information for teams in a specified league.

    Args:
        league (str): The name of the league for which stadium information is to be fetched.

    Attributes:
        schema_name (str): The formatted name of the league used as a schema in the database.
        url (str): The base URL for fetching team data from the Fotmob API.
        inserted_stadiums (int): The count of stadiums successfully inserted into the database.
        total_teams (int): The total number of teams in the specified league.
    """
    def __init__(self, league: str):
        super().__init__()
        self.schema_name = format_string(league)

        self.url = 'https://www.fotmob.com/api/teams?id='
        self.inserted_stadiums = 0
        self.total_teams = 0

    def extract_teams(self, connection: extensions.connection) -> List[Tuple[Optional[int]]]:
        """
        Extracts team IDs from the database.

        Args:
            connection (extensions.connection): Database connection object.

        Returns:
            List of tuples, each containing a team ID.
            Returns an empty list if no teams are found.
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT team_id FROM {self.schema_name}.teams;')
                teams_id = cursor.fetchall()
                return teams_id
        except Exception as e:
            LOGGER.error(f'Error extracting teams: {str(e)}.')
            return []

    @staticmethod
    def _format_date(date: Optional[int]) -> Optional[int]:
        """
        Formats a year ensuring it falls within a valid range.

        Args:
            date (int): The year to format and validate.
        
        Returns:
            The original year if valid, otherwise None.
        """
        if date is not None:
            if date > datetime.now().year or date < 1700:
                return None
        return date

    def get_stadiums(self, id: int) -> Optional[List[Optional[Union[str, int, float]]]]:
        """
        Retrieves stadium information from an external API based on the provided ID.

        Args:
            id (int): The ID of the stadium to retrieve information for.
        
        Returns:
            A list containing stadium information.
        """
        try:
            stadium_url = f'{self.url}{id}'
            json_content = self.fetch_data(stadium_url, 'json')
            if not json_content:
                LOGGER.warning(f'Failed to retrieve the json from '
                               f'the provided link "{stadium_url}" for schema "{self.schema_name}".')
                return None
        except Exception as e:
            LOGGER.warning(f'Failed to fetch data: {e}.')
            return None
        
        stadium = json_content.get('overview', {}).get('venue')

        if stadium:
            first_block = stadium.get('widget', {})
            second_block = stadium.get('statPairs', [])
            
            return [
                first_block.get('name'),
                first_block.get('city'),
                second_block[1][1] if len(second_block) > 1 else None,
                self._format_date(second_block[2][1] if len(second_block) > 2 else None),
                second_block[0][1] if len(second_block) > 0 else None,
                float(first_block.get('location', [None, None])[0]),
                float(first_block.get('location', [None, None])[1])
            ]
        
        return None

    def start_parse(self):
        """
        Parse and extract stadium data for the specified league and insert it into the database.
        """
        with connect_to_database() as connection:
            teams_id = self.extract_teams(connection)

            if teams_id:
                # Add 1 because the stadium table still contains
                # a row indicating the absence of information about the stadium
                self.total_teams = len(teams_id) + 1

                with ThreadPoolExecutor() as executor:
                    stadiums = executor.map(lambda id: self.get_stadiums(id[0]), teams_id)
                    stadiums_league = [stadium for stadium in list(stadiums) if stadium is not None]

                    # Insert the row ['Undefined', None, None, None, None, None, None] into the database
                    # because some matches lack stadium information to avoid exceptions
                    stadiums_league.append(['Undefined', None, None, None, None, None, None])
                    
                    try:
                        insert_data(connection, self.schema_name, 'stadiums', stadiums_league)
                        self.inserted_stadiums = len(stadiums_league)
                    except Exception:
                        LOGGER.error(f'Stadiums data for league "{self.schema_name}" was not inserted.')
        
        LOGGER.info(f'Successfully inserted {self.inserted_stadiums} stadiums out of {self.total_teams} ' \
                    f'into the table "{self.schema_name}.stadiums" (Some teams lack stadium data).')
                    

def main(league: str) -> None:
    fotmob_stadiums = FotmobStadiums(league)
    fotmob_stadiums.start_parse()
