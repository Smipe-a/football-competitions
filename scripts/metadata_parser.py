from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from datetime import datetime, timedelta
from typing import Optional, Union
from bs4 import BeautifulSoup
import requests
import os
import re

LOG_FILE_NAME = 'metadata_parser'
LOGGER = configure_logger(__name__, LOG_FILE_NAME)

RESOURCE_CATALOG = 'resources'
RESOURCE_DIRECTORY = os.path.join(os.path.abspath(
                        os.path.join(os.path.dirname(__file__), os.pardir)
                     ), RESOURCE_CATALOG)
RESOURCE_FILE_NAME = 'tournaments_metadata.json'


class ChampionatParser:
    def __init__(self, list_competition: Union[list, str], json_object: JsonHelper):
        self.list_competition = list_competition
        self.json_object = json_object

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }
        self.url = 'https://www.championat.com'

    @staticmethod
    def format_num(tour: str) -> int:
        """
        Format the tour string to extract and return the numeric part as an integer.

        Example:
            - "Tour: 21" -> 21

        Args:
            tour (str): The input string representing the tour.

        Returns:
            int: The extracted numeric part of the tour string as an integer.
        """
        num = re.search(r'\d+', tour)

        if num:
            return int(num.group())

    @staticmethod
    def add_last_date(date: str) -> str:
        """
        Calculate and add the last start date.

        Example:
            - "2022-01-01" -> "2022-01-08" (last date + 7 days)

        Args:
            date (str): The input date string in the format 'yyyy-mm-dd'.

        Returns:
            str: The calculated last start date in the format 'yyyy-mm-dd'.
        """
        last_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=7)
        return last_date.strftime('%Y-%m-%d')

    @staticmethod
    def format_date(date_time: str) -> str:
        """
        Format the date string from the given format 'dd.mm.yyyy' to 'yyyy-mm-dd'.

        Example:
            - "11.08.2023 22:00" -> "2023-08-11"

        Args:
            date_time (str): The input date string in the format 'dd.mm.yyyy'.

        Returns:
            str: The formatted date string in the format 'yyyy-mm-dd'.
        """
        date = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_time)

        if date:
            day, month, year = date.groups()
            formatted_date = f'{year}-{month}-{day}'
            return formatted_date

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
        else:
            LOGGER.error(f'Error {response.status_code} occurred while fetching {url}.')
            return None

    def parse_metadate(self, html_content: Optional[bytes]) -> None:
        """
        Parse metadata from HTML content, collecting attribute such as 'url'.

        Args:
            html_content (Optional[bytes]): The HTML content to parse.
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            competitions_block = soup.find_all('tr', class_='fav-item js-fav-item')

            if competitions_block:
                metadata = self.json_object.get()

                # Generating a dictionary for convenient comparison with HTML code elements
                # Example: {"Англия - Премьер-лига": "premier_league", ... }
                ru_name_competition = {
                    metadata[competition]['ru_name']: competition for competition in self.list_competition
                }

                for element in competitions_block:
                    # Iterating through all Russian competition names and adding the ones we are searching for
                    link_element = element.find('a', class_='table-item')
                    ru_name = link_element.find('span', class_='table-item__name').get_text(strip=True)

                    if ru_name in ru_name_competition:
                        self.json_object.append(ru_name_competition[ru_name], 'url',
                                                self.url + link_element.get('href') + 'calendar/')

                # Recording all found links
                self.json_object.write()
            else:
                LOGGER.warning("No competitions found in the HTML content.")
        except Exception as e:
            LOGGER.error(f'Error parsing metadata: {e}')

    def url_parse(self):
        """
        Parse the URL to obtain competitions data.

        Constructs the URL, fetches HTML content, and searches for the link to competitions.
        If successful, it retrieves the link and parses the metadata from the competitions page.

        The generated URL structure:
            https://www.championat.com/stat/football/tournaments/id/domestic
            where the id is randomly generated every year.

        Example:
            - Input URL: https://www.championat.com
            - Resulting URL for 2023 year: https://www.championat.com/stat/football/tournaments/453/domestic
        """
        url_category = self.url + '/stat/football'
        html_content = self.get_html(url_category)

        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find tag <a> with text "Страны"
            tag_name, tag_attribute = 'a', 'Страны'
            countries_block = soup.find(tag_name, string=tag_attribute)

            if countries_block:
                path_part = countries_block.get('href')
                url_countries = self.get_html(self.url + path_part)

                if url_countries:
                    self.parse_metadate(url_countries)
                else:
                    LOGGER.error('Failed to retrieve the link to the competitions page')
            else:
                LOGGER.warning(f'Url {url_category} with current tag "{tag_name}" '
                               f'and attribute "{tag_attribute}" not found')
        else:
            LOGGER.warning(f'Content current url "{url_category}" not found')


class DatesParser(ChampionatParser):
    def __init__(self, competition: str, json_object: JsonHelper):
        super().__init__(competition, json_object)
        self.competition = competition
        self.json_object = json_object

    def parse_metadate(self, html_content: Optional[bytes]) -> None:
        """
        Parse metadata from HTML content, collecting attributes such as 'start_date', 'year', and 'teams'.

        Args:
            html_content (Optional[bytes]): The HTML content to parse.
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find tag <span> with text "Участники"
        tag_name, tag_attribute = 'span', 'Участники'
        participants_block = soup.find(tag_name, string=tag_attribute)

        if participants_block:
            # Find tag 'div' in parent block with tag 'li'
            tag_name_parent, tag_name = 'li', 'div'
            value_block = participants_block.find_parent(tag_name_parent).find(tag_name)

            if value_block:
                participants_number = self.format_num(value_block.text)
                self.json_object.append(self.competition, 'teams', participants_number)
            else:
                LOGGER.warning(f'Metadata with parent tag "{tag_name_parent}" and current tag "{tag_name}" not found')

            # Attention! Not for all competitions!
            # Formula for calculating the number of rounds:
            # QUANTITY_TOURS = (QUANTITY_TEAMS - 1) * 2 and right shift
            quantity_teams = self.json_object.get(self.competition, 'teams')
            inf_date = '9999-99-99'
            start_date_tours = {tour: inf_date for tour in range(1, quantity_teams * 2 - 1)}

            # Find tag <tr> with attribute 'class'
            tag_name, tag_attribute = 'tr', 'stat-results__row'
            rows = soup.find_all(tag_name, class_=tag_attribute)
            if rows:
                for row in rows:
                    tour_num_tag = row.find('td', class_='stat-results__tour-num _hidden-td')
                    date_time_tag = row.find('td', class_='stat-results__date-time')

                    if tour_num_tag is not None and date_time_tag is not None:
                        tour_num = self.format_num(tour_num_tag.get_text(strip=True))
                        date_time = self.format_date(date_time_tag.get_text(strip=True))

                        # Finding the first start date of each round
                        start_date_tours[tour_num] = min(start_date_tours[tour_num], date_time)

                start_date_tours = list(start_date_tours.values())
                # Adding another date that is 7 days later than the last date of the competition
                # Check documentation
                start_date_tours.append(self.add_last_date(start_date_tours[-1]))

                self.json_object.append(self.competition, 'year', int(start_date_tours[0][:4]))
                self.json_object.append(self.competition, 'start_date', start_date_tours)
                self.json_object.write()

                LOGGER.info(f'Successfully obtained "{len(start_date_tours)}" '
                            f'start dates of "{self.competition}" competition gameweeks.')
                LOGGER.info(f'Attributes "year", "start_date" have been successfully gathered')
            else:
                LOGGER.warning(f'Metadata with current tag "{tag_name}" and attribute "{tag_attribute}" not found')
        else:
            LOGGER.warning(f'Metadata with current tag "{tag_name}" and attribute "{tag_attribute}" not found')


def main(competitions: list) -> None:
    json_object = JsonHelper()
    competition_data = json_object.read(os.path.join(RESOURCE_DIRECTORY, RESOURCE_FILE_NAME))

    championat_parser = ChampionatParser(competitions, competition_data)
    # We retrieve the main metadata parameter 'url' for
    # each competition from the ChampionatParser class for further searching of gameweek dates
    championat_parser.url_parse()

    for competition in competitions:
        url = competition_data.get(competition, 'url')

        dates_parser = DatesParser(competition, competition_data)
        html_content = dates_parser.get_html(url)

        if html_content:
            # Retrieving metadata 'teams', 'start_date', 'year' for each competition from the metadata file
            dates_parser.parse_metadate(html_content)
