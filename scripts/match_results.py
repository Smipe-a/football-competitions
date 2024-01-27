from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional
import requests
import re
import os

LOG_FILE_NAME = 'match_results.log'
LOGGER = configure_logger(__name__, LOG_FILE_NAME)

PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG = 'resources'
METADATA_FILE_NAME = 'tournaments_metadata.json'


class SkySportsParser:
    def __init__(self, competition: str, json_object: JsonHelper, year: int, lower_date: str, upper_date: str):
        self.competition = competition
        self.json_object = json_object
        self.year = year
        self.lower_date = lower_date
        self.upper_date = upper_date

        self.season = f'{year}-{str(year + 1)[2:]}'
        self.main_url = 'https://www.skysports.com/'
        self.url = f'{self.main_url}{self.competition.replace("_", "-")}-results/{self.season}'

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

    @staticmethod
    def format_month(month_name: str) -> Optional[str]:
        """
        Converts a month name from string to numeric format.

        Example:
            - 'January' -> '01'

        Args:
            month_name (str): The name of the month in full string format (e.g., 'January').

        Returns:
            str: The numeric representation of the month (e.g., '01' for 'January'), otherwise None.
        """
        try:
            month = datetime.strptime(month_name, '%B')
            return month.strftime('%m')
        except ValueError:
            LOGGER.warning('The month name is misspelled in string format.')
            return None

    def format_date(self, date: list, year: str) -> Optional[str]:
        """
        Extracts and formats a date from the given list and year.

        Example:
            - (['15', 'January'], '2024') -> '2024-01-15'

        Args:
            date (list): A list containing day and month information in string format.
            year (str): The year in string format.

        Returns:
            str: The formatted date in 'YYYY-MM-DD' string format. Otherwise, None.
        """
        day = re.sub(r'\D', '', date[0])
        month = self.format_month(date[1])

        if month:
            date = datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')
            return date.strftime('%Y-%m-%d')

        return None

    def into_values_to_db(self, cursor, table_name: str, values: list) -> bool:
        """
        Inserts values into the database.

        Args:
            cursor: Cursor object for executing database operations.
            table_name (str): Name of the table where the values will be inserted.
            values (list): List of values to be inserted into the table.

        Returns:
            bool: Returns True upon successful insertion, False in case of an error.
        """
        try:
            # Define SQL queries for inserting data into different tables based on the 'table_name'
            insert_query = {
                'team_clashes': f"""
                    INSERT INTO {self.competition}.{table_name} VALUES (
                        %s, %s, %s, %s
                    ) ON CONFLICT DO NOTHING;
                    """,
                'result_clashes': f"""
                    INSERT INTO {self.competition}.{table_name} VALUES (
                        %s, %s, %s
                    ) ON CONFLICT DO NOTHING;
                    """,
                'info_clashes': f"""
                    INSERT INTO {self.competition}.{table_name} VALUES (
                        %s, %s
                    ) ON CONFLICT DO NOTHING;
                    """
            }

            # Execute a bulk insert into the database using the pre-defined SQL query
            cursor.executemany(insert_query[table_name], values)
            return True

        except Exception as e:
            LOGGER.warning(f'Error inserting data into {self.competition}.{table_name}: {str(e).strip()}.')
            return False

    def parse_match_results(self):
        """
        Parses match results from HTML content and inserts them into the database.

        This function extracts match details, inserts data into the database,
        and updates JSON objects with statistics URLs.
        """
        html_content = self.get_html(self.url)

        if not html_content:
            LOGGER.warning(f'The main content of the page at the link "{self.url}" was not retrieved.')
            return

        soup = BeautifulSoup(html_content, 'html.parser')

        tag_name, attribute_name = 'div', 'fixres__body'
        try:
            clashes_block = soup.find(tag_name, class_=attribute_name)
        except AttributeError:
            LOGGER.warning(f'Block results matches with tag_name "{tag_name}" and '
                           f'attribute_name "{attribute_name}" not found.')
            return

        attribute_name = 'fixres__item'
        try:
            matches = clashes_block.find_all(tag_name, class_=attribute_name)
        except AttributeError:
            LOGGER.warning(f'List results matches with tag_name "{tag_name}" and '
                           f'attribute_name "{attribute_name}" not found.')
            return

        database_table_name = {
            'team_clashes': [],
            'result_clashes': [],
            'info_clashes': []
        }

        # Lists for JSON files are being initialized
        stats_players_url, stats_teams_url = [], []
        # We count the number of matches found within the specified time interval
        matchweek = 0

        for match in matches:
            try:
                # We extract the date as a string and split it into components for further formatting
                # Example: {Input: Sunday 21st January} {Output: [21st, January]}
                month_with_day = match.find_previous('h4', class_='fixres__header2').text.strip().split(' ')[1:]
                # Example: {Input: January 2024} {Output: 2024}
                year = match.find_previous('h3', class_='fixres__header1').text.strip().split()[-1]

                # We format the string as 'YYYY-MM-DD'
                match_date = self.format_date(month_with_day, year)

                # We retrieve dates only for the required matchweek
                if self.lower_date <= match_date <= self.upper_date:
                    match_id = int(match.find('a', class_='matches__item')['data-item-id'])
                    home_team = match.find(class_='matches__participant--side1').text.strip()
                    away_team = match.find(class_='matches__participant--side2').text.strip()
                    score_ht = int(match.find(class_='matches__teamscores-side').text.strip())
                    score_at = int(match.find(class_='matches__teamscores-side').find_next_sibling().text.strip())
                    time = match.find(class_='matches__date').text.strip()

                    database_table_name['team_clashes'].append([
                        match_id,
                        self.year,
                        home_team,
                        away_team
                    ])

                    database_table_name['result_clashes'].append([
                        match_id,
                        score_ht,
                        score_at
                    ])

                    database_table_name['info_clashes'].append([
                        match_id,
                        match_date + ' ' + time
                    ])

                    path_teams = f'{home_team.replace(" ", "-").lower()}-vs-{away_team.replace(" ", "-").lower()}'
                    stats_players_url.append(f'{self.main_url}football/{path_teams}/teams/{match_id}')
                    stats_teams_url.append(f'{self.main_url}football/{path_teams}/stats/{match_id}')

                    matchweek += 1

            except Exception as e:
                LOGGER.warning(f'Unexpected error processing match data: {str(e)}.')
                return

        with connect_to_database() as connection, connection.cursor() as cursor:
            for table_name in database_table_name.keys():
                is_status = self.into_values_to_db(cursor, table_name, database_table_name[table_name])

                if is_status:
                    connection.commit()
                    LOGGER.info(f'Successfully inserted {matchweek} '
                                f'rows into the schema {self.competition} and table {table_name}.')
                else:
                    LOGGER.warning(f'Part of the data for competition {self.competition} '
                                   f'was not added to the table {table_name} or encountered an error.')
                    connection.rollback()

        self.json_object.append(self.competition, 'skysports_players_urls', stats_players_url)
        self.json_object.append(self.competition, 'skysports_teams_urls', stats_teams_url)


def main(competition: str, upper_date: str):
    # Reading and extracting the year from a JSON file, which we then convert into a season
    metadata_json = JsonHelper()
    metadata_json.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, METADATA_FILE_NAME))
    year = metadata_json.get(competition, 'year')

    # Reading and extracting the date from a JSON file from which data about matches should be obtained
    competition_urls_json = JsonHelper()
    competition_urls_json.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, f'{competition}_urls.json'))
    lower_date = competition_urls_json.get(competition, 'prev_date')

    sky_sports_parser = SkySportsParser(competition, competition_urls_json, year, lower_date, upper_date)
    sky_sports_parser.parse_match_results()

    # We update the previous date from which the search begins
    competition_urls_json.append(competition, 'prev_date', upper_date)
    competition_urls_json.write()
