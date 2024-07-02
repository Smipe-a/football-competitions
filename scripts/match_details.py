from typing import Optional, Union, Tuple, Any, Set, List
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extensions

from utils.database.connector import connect_to_database, insert_data
from utils.constants import MATCH_DETAILS_FILE_LOG
from utils.link_mapper import format_string
from utils.logger import configure_logger
from utils.fetcher import Fetcher

LOGGER = configure_logger(__name__, MATCH_DETAILS_FILE_LOG)


class FotmobDetails(Fetcher):
    """
    A class used to fetch detailed match data from FOTMOB API.

    Args:
        name_schema (str): The name of the database schema where data will be stored.

    Attributes:
        url (str): The base URL for the FOTMOB match details API.
        inserted_results (int): Number of rows successfully inserted into 'match_results' table.
        inserted_lineups (int): Number of rows successfully inserted into 'match_lineups' table.
        inserted_details (int): Number of rows successfully inserted into 'match_details' table.
    """
    def __init__(self, name_schema: str):
        super().__init__()
        self.name_schema = name_schema

        self.url = 'https://www.fotmob.com/api/matchDetails?matchId='
        self.inserted_results = 0
        self.inserted_lineups = 0
        self.inserted_details = 0

    def _get_data(self, connection: extensions.connection,
                  table_name: str, match_id: int,
                  data: Any, stadiums: Set[str]) -> Optional[List[Optional[Union[int, str]]]]:
        """
        Process and extract specific data fields from a given match data dictionary.

        Args:
            connection (extensions.connection): Database connection object for executing queries.
            table_name (str): Name of the table where data will be inserted.
            match_id (int): ID of the match for which data is being processed.
            data (Any): Dictionary containing match data fetched from the API.
            stadiums (Set[str]): Set containing names of stadiums already encountered.

        Returns:
            Extracted data formatted as a list for insertion
            into the specified table, or None if the table_name provided is invalid.
        """
        content = data['content']
        if content:
            stadium = content['matchFacts']['infoBox']['Stadium']
            attendance = content['matchFacts']['infoBox']['Attendance']
            if stadium:
                stadium = stadium.get('name', None)
        else:
            LOGGER.info(f'Stadium and attendance data are missing for match {match_id}.')
            stadium, attendance = None, None
        
        # If the stadium is absent in the 'stadiums' table, first add the new stadium name there
        if stadium and stadium not in stadiums:
            stadiums.add(stadium)
            new_stadium = [stadium, None, None, None, None, None, None]
            insert_data(connection, self.name_schema, 'stadiums', [new_stadium])

        lineup = data['content']['lineup2']
        if lineup:
            lineup_ht = lineup['homeTeam'].get('formation', None)
            lineup_at = lineup['awayTeam'].get('formation', None)
        else:
            lineup_ht = None
            lineup_at = None

        table = {
            'match_results': [
                match_id,
                data['header']['teams'][0]['score'],
                data['header']['teams'][1]['score']
            ],
            'match_lineups': [
                match_id,
                lineup_ht,
                lineup_at
            ],
            'match_details': [
                match_id,
                data['general']['matchTimeUTCDate'].replace('T', ' ')[:-1],
                stadium,
                attendance,
                data['header']['status']['reason']['long']
            ]
        }

        if table_name in table:
            return table[table_name]

        return None

    def start_parse(self, connection: extensions.connection,
                    matches: List[Tuple[int]], stadiums: Set[str]) -> None:
        """
        Fetches match data from FOTMOB API, parses JSON content for
        'match_results', 'match_lineups', and 'match_details' tables,
        inserts data into the database, updates counters.
        """
        results, lineups, details = [], [], []

        for match_id in matches:
            match_id = match_id[0]

            try:
                fotmob_match_url = f'{self.url}{match_id}'
                json_content = self.fetch_data(fotmob_match_url, 'json')
                if not json_content:
                    LOGGER.warning(f'Failed to retrieve the element ' \
                                   f'code from the provided link "{fotmob_match_url}".')
                    return None
            except Exception as e:
                LOGGER.warning(f'Failed to fetch data: {e}.')
                return None
            
            # If there is no match data, the API will return a JSON of the form:
            # {"error": true, "message": "Data not found", "matchId": match_id}
            # If there is no error, parse the data.
            if not json_content.get('error', False):
                results.append(self._get_data(connection, 'match_results', match_id, json_content, stadiums))
                lineups.append(self._get_data(connection, 'match_lineups', match_id, json_content, stadiums))
                details.append(self._get_data(connection, 'match_details', match_id, json_content, stadiums))

        try:
            results = [match for match in results if match is not None]
            lineups = [match for match in lineups if match is not None]
            details = [match for match in details if match is not None]

            insert_data(connection, self.name_schema, 'match_results', results)
            insert_data(connection, self.name_schema, 'match_lineups', lineups)
            insert_data(connection, self.name_schema, 'match_details', details)

            self.inserted_results += len(results)
            self.inserted_lineups += len(lineups)
            self.inserted_details += len(details)
        except Exception as e:
            LOGGER.error('The data was not successfully inserted.', e)
            

def _batch(matches: List[Tuple[Optional[int]]], 
           batch_size: int = 50) -> List[List[Tuple[Optional[int]]]]:
    return [matches[i:i + batch_size] for i in range(0, len(matches), batch_size)]

def main(league: str):
    with connect_to_database() as connection, connection.cursor() as cursor:
        name_schema = format_string(league)
        fotmob_details = FotmobDetails(name_schema)

        # If we want to get data for past game seasons, then 'season != MAX(season)'
        query = f"""
                SELECT match_id
                FROM {name_schema}.matches
                WHERE season = (SELECT MAX(season) FROM {name_schema}.matches);
                """
        cursor.execute(query)
        match_ids = cursor.fetchall()
        total_data = len(match_ids)

        query = f'SELECT stadium FROM {name_schema}.stadiums;'
        cursor.execute(query)
        stadiums = cursor.fetchall()
        cursor.close()

        # In the Fotmob source, the data on stadiums is incomplete.
        # They are not available on the team pages but can be found in the list matches.
        # Therefore, we need to process them separately to avoid foreign key exceptions
        stadiums = set(stadium[0] for stadium in stadiums)

        # 'match_ids' data is batched into 50 packets to avoid overloading the database
        with ThreadPoolExecutor() as executor:
            executor.map(
                lambda matches: fotmob_details.start_parse(connection, matches, stadiums), 
                _batch(match_ids)
            )
        
        LOGGER.info(f'For the schema "{fotmob_details.name_schema}", ' \
                    f'{fotmob_details.inserted_results}, {fotmob_details.inserted_lineups}, ' \
                    f'and {fotmob_details.inserted_details} rows of data were successfully ' \
                    f'inserted into the tables "match_results", "match_lineups", and "match_details" ' \
                    f'respectively out of a total possible matches from {total_data}.')
