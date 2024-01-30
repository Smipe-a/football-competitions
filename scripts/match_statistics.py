from utils.db_connector import connect_to_database
from concurrent.futures import ThreadPoolExecutor
from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from bs4 import BeautifulSoup
from typing import Optional
import requests
import re
import os

LOG_FILE_NAME = 'match_statistics.log'
LOGGER = configure_logger(__name__, LOG_FILE_NAME)

PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG = 'resources'


class SkySportsParser:
    def __init__(self, connection, competition: str, transfermarkt_urls: dict):
        self.connection = connection
        self.competition = competition
        self.transfermarkt_urls = transfermarkt_urls

        self.updated_values = 0
        self.added_values = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_html(self, url: str) -> Optional[bytes]:
        """
        Fetch HTML content from the specified URL.

        Args:
            url (str): The URL from which to fetch the HTML content.

        Returns:
            Optional[bytes]: The fetched HTML content as bytes or None if an error occurs.
        """
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.content
        elif response.status_code == 404:
            self.get_html(url)
        else:
            LOGGER.error(f'Error {response.status_code} occurred while fetching {url}.')
            return None

    @staticmethod
    def extract_num(text: str) -> Optional[int]:
        """
        Helper method to extract a number from the text.

        Args:
            text (str): Text from which to extract the number.

        Returns:
            Optional[int]: Extracted number (if possible), otherwise None.
        """
        try:
            return int(''.join(filter(lambda x: x.isdigit(), text)))
        except ValueError:
            return None

    def insert_data_in_database(self, cursor, table_name: str, values: list) -> bool:
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
                'match_statistics': f"""
                    INSERT INTO {self.competition}.{table_name} VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT DO NOTHING;
                    """
            }

            # Execute a bulk insert into the database using the pre-defined SQL query
            cursor.executemany(insert_query['match_statistics'], values)
            self.added_values += 1
            return True

        except Exception as e:
            LOGGER.warning(f'Error inserting data into {self.competition}.{table_name}: {str(e).strip()}.')
            return False

    def update_data_in_database(self, cursor, match_id: int, stadium_value: str, attendance_value: int) -> bool:
        """
        Update stadium and attendance values for a given match in the 'info_clashes' table.

        Args:
            cursor: Database cursor to execute the SQL query.
            match_id (int): Unique identifier for the match.
            stadium_value (str): New value for the 'stadium' column.
            attendance_value (int): New value for the 'attendance' column.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        try:
            # Define SQL queries for updating data in the 'info_clashes' table
            update_query = f"""
                UPDATE {self.competition}.info_clashes
                    SET stadium = %s, attendance = %s
                    WHERE match_id = %s
                """

            # Execute the update query in the database
            cursor.execute(update_query, (stadium_value, attendance_value, match_id))
            self.updated_values += 1
            return True

        except Exception as e:
            LOGGER.warning(f'Error updating data in "{self.competition}.info_clashes": {str(e).strip()}.')
            return False

    @staticmethod
    def get_match_id(url: str) -> Optional[int]:
        """
        Extracts the match_id (last 6 digits) from the provided URL.

        Example:
            - "www.skysports.com/football/1.-fc-union-berlin-vs-rb-leipzig/teams/488348" -> 488348

        Args:
            url (str): The URL from which to extract the match_id.

        Returns:
            Optional[int]: The match_id as an integer, or None if extraction fails.
        """
        match_id = re.search(r'(\d{6})$', url)

        if match_id:
            return int(match_id.group(1))
        else:
            LOGGER.warning(f'Failed to retrieve "match_id" from the link "{url}".')
            return None

    @staticmethod
    def get_stadium(info_block) -> Optional[str]:
        """
        Extracts the stadium information from the HTML block.

        Args:
            info_block: HTML block with match information.

        Returns:
            Optional[str]: Stadium name (if available), otherwise an empty string.
        """
        # We check 'stadium' for the absence of a block in the HTML
        tag_name, attribute_name = 'span', 'sdc-site-match-header__detail-venue--with-seperator'
        stadium_element = info_block.find(tag_name, class_=attribute_name)

        # The search is conducted using the SkySports as the source
        if not stadium_element:
            try:
                LOGGER.warning(f'Stadium information not found in primary source (SkySports) with '
                               f'tag "{tag_name}" and attribute "{attribute_name}".')

                attribute_name = 'sdc-site-match-header__detail-venue'
                LOGGER.info(f'Attempting to search with tag "{tag_name}" and attribute "{attribute_name}".')

                # The last character is not considered because the page
                # element's code for the stadium name includes a period (".") at the end
                # Example 'Stade Bollaert-Delelis.'
                return info_block.find(tag_name, class_=attribute_name).text.strip()[:-1]

            except AttributeError:
                LOGGER.warning('The stadium name is not present in the SkySports source. '
                               'The search will proceed in the Transfermarkt source.')
                # I will add the method as soon as
                # I find a link with match statistics that is not available on SkySports
                return None

        return stadium_element.text.strip()

    def get_attendance(self, info_block, url: str) -> Optional[int]:
        """
        Method for extracting attendance information (if available) from an HTML block.

        Args:
            info_block: HTML block with match information.
            url: URL of the match page.

        Returns:
            int: Number of attendees (if available), otherwise None.
        """
        # Check for the presence of the 'attendance' block in HTML
        attendance_element = info_block.find('span', class_='sdc-site-match-header__detail-attendance')

        # If the number of viewers on the SkySports page is not detected,
        # we attempt to retrieve data from the Transfermarkt page
        if attendance_element:
            attendance_text = attendance_element.text.strip()

            # Use a helper method to extract the number from the text
            return self.extract_num(attendance_text)
        else:
            # Use a regular expression to extract the match key from the URL
            # Example: 'https://www.skysports.com/football/lille-vs-metz/teams/487403' -> 'lille-vs-metz'
            skysports_key = re.search(r'([^/]+)-vs-([^/]+)', url).group()
            transfermarkt_url = self.transfermarkt_urls.get(skysports_key, '')

            if transfermarkt_url == '':
                LOGGER.warning(f'Transfermarkt URL not found for key "{skysports_key}" in LinkMapper.')
                return None

            # Get the HTML content from Transfermarkt
            html_content = self.get_html(transfermarkt_url)
            soup_tm = BeautifulSoup(html_content, 'html.parser')

            try:
                # Extract the attendance text from the Transfermarkt page
                attendance_text = soup_tm.find('p', class_='sb-zusatzinfos').find('strong').text.strip()
            except Exception as e:
                LOGGER.warning(f'Failed to extract attendance from Transfermarkt: {str(e).strip()}.')
                return None

            return self.extract_num(attendance_text)

    @staticmethod
    def transform_list(team: list) -> list:
        """
        Removes redundant data such as total_shots, yellow_card, and red_card
        from the input list.

        Args:
            team (list): Input list containing team data.

        Returns:
            list: Transformed list with redundant data removed.
        """
        return [team[:2] + team[3:-2]]

    @staticmethod
    def append_statistics(block_events, team_statistics: list):
        """
        Appends statistics values from the provided block of events to the given team_statistics list.

        Args:
            block_events: Block of events containing statistics information.
            team_statistics (list): List to which the statistics values will be appended.
        """
        for event in block_events:
            value_event = event.find(class_='sdc-site-match-stats__val')
            try:
                team_statistics.append(int(value_event.text.strip()))
            except ValueError:
                team_statistics.append(float(value_event.text.strip()))

    def parse_statistics(self, url: str):
        """
        Parses match statistics from the provided URL and updates the database.

        Args:
            url (str): The URL containing match statistics information.
        """
        html_content = self.get_html(url)

        if not html_content:
            LOGGER.warning(f'The link "{url}" does not have an element code in the SkySports source. '
                           f'Attempting to find match statistics in the Transfermarkt source.')
            # I will add the method as soon as I find a link with match statistics that is not available on SkySports
            return None

        soup = BeautifulSoup(html_content, 'html.parser')

        head_match_info = soup.find(class_='sdc-site-match-header__detail')

        match_id = self.get_match_id(url)
        stadium = self.get_stadium(head_match_info)
        attendance = self.get_attendance(head_match_info, url)

        if match_id == 489311:
            print(match_id, stadium, attendance)

        with self.connection.cursor() as cursor:
            self.update_data_in_database(cursor, match_id, stadium, attendance)

        # Initializing lists to store match statistics values for the away and home teams
        home_team = [match_id]
        away_team = [match_id]

        # Finding the block in the HTML with the statistics and extracting it
        table_statistics = soup.find(class_='sdc-site-match-stats__inner')

        home_statistics = table_statistics.find_all(class_='sdc-site-match-stats__stats-home')
        self.append_statistics(home_statistics, home_team)

        away_statistics = table_statistics.find_all(class_='sdc-site-match-stats__stats-away')
        self.append_statistics(away_statistics, away_team)

        with self.connection.cursor() as cursor:
            self.insert_data_in_database(cursor, 'home_match_statistics', self.transform_list(home_team))
            self.insert_data_in_database(cursor, 'away_match_statistics', self.transform_list(away_team))

        self.connection.commit()


def main(competition: str):
    competition_urls = JsonHelper()
    # Open the json file that contains URLs of all matches current competition
    competition_urls.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, f'{competition}_urls.json'))

    # Obtain a list of links necessary for extracting data and adding it to the database
    skysports_urls = competition_urls.get(competition, 'skysports_teams_urls')
    # Obtain a hashmap of links in order to supplement missing data in the SkySports source
    transfermarkt_hashmap = competition_urls.get(competition, 'transfermarkt_urls')

    with connect_to_database() as connection:
        sky_sports_parser = SkySportsParser(connection, competition, transfermarkt_hashmap)

        with ThreadPoolExecutor() as executor:
            executor.map(sky_sports_parser.parse_statistics, skysports_urls)

    LOGGER.info(f'In the schema "{competition}", '
                f'"{sky_sports_parser.updated_values}" rows of data have been updated for the table "info_clashes".')

    LOGGER.info(f'In the schema "{competition}", "{sky_sports_parser.added_values}" new rows '
                f'were added to the tables "home_match_statistics" with "away_match_statistics".')
