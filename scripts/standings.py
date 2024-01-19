from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from bs4 import BeautifulSoup
from typing import Optional
from psycopg2 import sql
import requests
import os

# Configure logger for the current module
LOG_FILE_NAME = 'standings.log'
LOGGER = configure_logger(__name__, LOG_FILE_NAME)

PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG = 'resources'
RESOURCE_FILE_NAME = 'tournaments_metadata.json'


class StandingsParser:
    def __init__(self, competition: str, json_object: JsonHelper):
        self.competition = competition
        self.json_object = json_object

    @staticmethod
    def get_html(url: str) -> Optional[bytes]:
        """
        Fetch HTML content from the specified URL.

        Args:
            url (str): The URL from which to fetch the HTML content.

        Returns:
            Optional[bytes]: The fetched HTML content as bytes or None if an error occurs.
        """
        response = requests.get(url)

        if response.status_code == 200:
            return response.content
        else:
            LOGGER.error(f'Error {response.status_code} occurred while fetching {url}.')
            return None

    def parse_standings(self, year: str) -> None:
        """
        Parse and extract standings data for a specific year.

        Args:
            year (str): The year for which standings data is to be parsed.
        """
        # Construct the URL for standings data
        url = f'https://www.skysports.com/{self.competition.replace("_", "-")}-table/{year}'

        try:
            html_content = self.get_html(url)

            if html_content:
                soup = BeautifulSoup(html_content, 'html.parser')
                table = soup.find('table', class_='standing-table__table')

                team_list = []
                # Each row represents general data about a team
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) > 0:
                        team_list.append(self.extract_data(cells))

                LOGGER.info(f'Successfully gathered "{len(team_list)}" '
                            f'rows of the table for the "{self.competition}" competition.')

                # Add the extracted team data to the database
                self.add_values_to_db(data_to_insert=team_list)

                LOGGER.info(f'The data has been successfully added to the "{self.competition}" '
                            f'schema and the "standings" table.')
            else:
                LOGGER.warning(f'HTML content with current url "{url}" not found.')

        except Exception as e:
            LOGGER.error(f'Error processing standings data: {e}.')

        finally:
            return None

    def extract_data(self, cells) -> Optional[list]:
        """
        Extract team data from HTML cells.

        Args:
            cells: List of BeautifulSoup ResultSet representing HTML cells.

        Returns:
            Optional[list]: List containing extracted team data, or None if team name is not found.
        """
        # Default tag and attribute for team name extraction
        tag_name, attribute_name = 'a', 'standing-table__cell--name-link'

        try:
            # Before adding to the list, it is necessary to remove the '*' symbol,
            # as it indicates additional information about the team on the website.
            team_name = cells[1].find(tag_name, class_=attribute_name).get_text(strip=True).replace('*', '')

        except AttributeError:
            LOGGER.warning(
                f'Name of team with current tag "{tag_name}" and attribute name "{attribute_name}" not found.')

            # Try finding the team name using an alternative tag and attribute
            tag_name, attribute_name = 'span', 'standing-table__cell--name-text'
            LOGGER.warning(f'Attempting to find the team name using '
                           f'the following tag "{tag_name}" and attribute {attribute_name}.')
            try:
                team_name = cells[1].find(tag_name, class_=attribute_name).get_text(strip=True).replace('*', '')

            except AttributeError:
                LOGGER.error(f'Name of team with current tag "{tag_name}" '
                             f'and attribute name "{attribute_name}" not found.')
                return None

        return [
            team_name,
            self.json_object.get(self.competition, 'year'),  # season
            cells[0].get_text(strip=True),  # position
            cells[3].get_text(strip=True),  # won
            cells[4].get_text(strip=True),  # drawn
            cells[5].get_text(strip=True),  # lost
            cells[6].get_text(strip=True),  # goals_for
            cells[7].get_text(strip=True),  # goals_against
            cells[9].get_text(strip=True)   # points
        ]

    def add_values_to_db(self, data_to_insert: list):
        """
        Add team data to the database.

        Args:
            data_to_insert (list): List containing team data to be inserted into the database.
        """
        connection = connect_to_database()
        cursor = connection.cursor()

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

            cursor.executemany(insert_query, data_to_insert)
            connection.commit()

        except Exception as e:
            connection.rollback()
            print(f'Error during the database operation: {e}.')

        finally:
            cursor.close()
            connection.close()


def main(competition: str):
    json_object = JsonHelper()
    json_object.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, RESOURCE_FILE_NAME))

    competition_parser = StandingsParser(competition, json_object)
    competition_parser.parse_standings(year=json_object.get(competition, 'year'))
