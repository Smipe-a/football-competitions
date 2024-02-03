from constants import (STANDINGS_FILE_LOG, LEAGUES, FOTMOB_SUFFIX_URL,
                       PROJECT_DIRECTORY, RESOURCE_CATALOG, COMPETITIONS_JSON)
from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from typing import Union, List
from fetcher import Fetcher
from psycopg2 import sql
import os

# Configure logger for the current module
LOGGER = configure_logger(__name__, STANDINGS_FILE_LOG)


class StandingsParser(Fetcher):
    def __init__(self, competition: str):
        super().__init__()
        self.competition = competition

        self.suffix_pattern = dict(zip(LEAGUES, FOTMOB_SUFFIX_URL))

    def start_parse(self, year: int) -> None:
        """
        Parse and extract standings data for a specific year.

        Args:
            year (int): The year for which standings data is to be parsed.
        """
        id_competition = self.suffix_pattern[self.competition][0]
        # Construct the URL for standings data
        url = f'https://www.fotmob.com/api/leagues?id={id_competition}&ccode3=KAZ&season={year}%2F{year + 1}'
        # Redesign the link string for Kazakhstan according to a different template
        if id_competition == 225:
            url = url[:-7]

        try:
            json_content = self.fetch_data(url, 'json')
            if not json_content:
                LOGGER.warning(f'Failed to retrieve the element code from the provided link "{url}".')
                return None

        except Exception as e:
            LOGGER.warning(str(e).strip())
            return None

        try:
            teams = json_content['table'][0]['data']['table']['all']
        except KeyError as key_err:
            LOGGER.warning(f'An error occurred in the JSON object: {str(key_err).strip()}.')
            LOGGER.warning(f'The data for the "{self.competition}" competition '
                           f'for the "{year}" season was not received.')
            return None

        team_list = []
        # Each row represents general data about a team
        for team in teams:
            goals_for, goals_against = team['scoresStr'].split('-')
            team_list.append([
                team['name'],        # team
                year,                # season
                team['idx'],         # position
                team['wins'],        # won
                team['draws'],       # drawn
                team['losses'],      # lost
                int(goals_for),      # goals_for
                int(goals_against),  # goals_against
                team['pts']          # points
            ])

        LOGGER.info(f'Successfully gathered "{len(team_list)}" '
                    f'rows of the table for the "{self.competition}" competition for the season "{year}".')

        self.into_values_to_db(team_list)

        LOGGER.info(f'The data has been successfully added to the "{self.competition}" '
                    f'schema and the "standings" table.')

    def into_values_to_db(self, insert_data: List[List[Union[str, int]]]) -> None:
        """
        Add team data to the database.

        Args:
            insert_data (List[List[Union[str, int]]]): List containing team data to be inserted into the database.
        """
        with connect_to_database() as connection, connection.cursor() as cursor:
            try:
                insert_query = sql.SQL(
                    """
                    INSERT INTO {}.standings VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (team, season)
                    DO UPDATE SET
                        position = EXCLUDED.position,
                        won = EXCLUDED.won,
                        drawn = EXCLUDED.drawn,
                        lost = EXCLUDED.lost,
                        goals_for = EXCLUDED.goals_for,
                        goals_against = EXCLUDED.goals_against,
                        points = EXCLUDED.points
                   """
                ).format(sql.Identifier(self.competition))

                cursor.executemany(insert_query, insert_data)
                connection.commit()

            except Exception as e:
                connection.rollback()
                LOGGER.warning(f'Error during the database operation: {e}.')


def main(competition: str) -> None:
    competition_json = JsonHelper()
    competition_json.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, COMPETITIONS_JSON))

    standings_parser = StandingsParser(competition)
    standings_parser.start_parse(competition_json.get(competition, 'year'))
