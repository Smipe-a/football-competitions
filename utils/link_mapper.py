from concurrent.futures import ThreadPoolExecutor
from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from bs4 import BeautifulSoup
from typing import Optional
import requests
import re
import os

# Define constants for logging and directory structure
LOG_FILE_NAME = 'link_mapper.log'
LOGGER = configure_logger(__name__, LOG_FILE_NAME)

PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG = 'resources'
METADATA_FILE_NAME = 'tournaments_metadata.json'


class LinkMapper:
    def __init__(self, competition: str, year: int):
        # Initialize LinkMapper with competition and year
        self.competition = competition
        self.year = year

        # Initialize LinkMapper with competition and year
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }
        # Initialize LinkMapper with competition and year
        self.part_url = {
            'premier_league': ['premier-league', 'GB1'],
            'la_liga': ['laliga', 'ES1'],
            'serie_a': ['serie-a', 'IT1'],
            'bundesliga': ['bundesliga', 'L1'],
            'ligue_1': ['ligue-1', 'FR1']
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
        else:
            LOGGER.error(f'Error {response.status_code} occurred while fetching {url}.')
            return None

    @staticmethod
    def format_string(team_name: str) -> str:
        """
        Formats the team name string according to the specified template.

        Example:
            - "Manchester United FC" -> "manchester-united"
            - "Parma Calcio 1913" -> "parma"

        Args:
            team_name (str): The team name to be formatted.

        Returns:
            str: The formatted team name.
        """
        # Dictionary with names of teams that do not follow the template below
        static_team_names = {
            'SV Darmstadt 98': 'sv-darmstadt-98',
            '1.FC Heidenheim 1846': '1-fc-heidenheim-1846',
            'Borussia Mönchengladbach': 'mgladbach',
            '1.FC Köln': 'cologne',
            'Stade Brestois 29': 'brest',
            'Stade Rennais FC': 'rennes',
            'RCD Mallorca': 'real-mallorca',
            'RCD Espanyol Barcelona': 'espanyol',
            'SD Huesca': 'sd-huesca',
            'AS Saint-Étienne': 'st-etienne',
            'Hertha BSC': 'hertha-berlin',
            '1.FC Nuremberg': 'fc-nurnberg',
            'FC Girondins Bordeaux': 'bordeaux',
            'FC Ausburg': 'fc-ausburg',
            'RC Lens': 'rc-lens',
            'Dijon FCO': 'dijon',
            '1.FC Union Berlin': '1-fc-union-berlin',
            'FC Augsburg': 'fc-augsburg',
            '1.FSV Mainz 05': 'mainz',
            'AC Ajaccio': 'ac-ajaccio',
            'AC Milan': 'ac-milan',
            'Genoa CFC': 'genoa',
            'SS Lazio': 'lazio',
            'ACF Fiorentina': 'fiorentina',
            'Chievo Verona': 'chievo'
        }

        team_name = static_team_names.get(team_name, team_name)

        # Remove specified patterns
        patterns_to_remove = [' FC', 'AFC ', ' AFC', 'SV ', '04 ', 'VfL ', 'TSG 1899 ', 'VfB ', 'LOSC ', ' Calcio',
                              'OGC ', ' Foot 63', 'AS ', 'RC ', ' Alsace', 'Olympique ', 'UD ', ' UD', ' CF', 'US ',
                              ' de', 'CA ', ' Balompié', 'Deportivo ', 'SD ', 'CD ', ' SCO', 'AJ ', 'ESTAC ', 'AC ',
                              ' AC', ' Olympique', ' SC', 'EA ', 'SM ', 'SpVgg ', ' 04', 'FC ', ' HSC', 'Stade ',
                              ' 1919', 'Hellas ', ' BC', 'SSC ', ' 1909', 'UC ', ' 1913']
        for pattern in patterns_to_remove:
            team_name = re.sub(pattern, '', team_name)

        # Handle special character replacements
        special_replacements = {'&': 'and', '.': '-', 'í': 'i', 'á': 'a', 'é': 'e', 'î': 'i', 'ü': 'u'}
        for char, replacement in special_replacements.items():
            team_name = team_name.replace(char, replacement)

        # Convert to lowercase and replace spaces with hyphens
        team_name = team_name.lower().replace(' ', '-')

        return team_name

    def create_keys(self, soup: BeautifulSoup) -> Optional[list]:
        """
        Create keys for the mapping dictionary.

        Args:
            soup (BeautifulSoup): BeautifulSoup object representing the HTML content.

        Returns:
            Optional[list]: List of keys for the mapping dictionary or None if an error occurs.
        """
        keys = []

        tag_name, attribute_name = 'tr', 'table-grosse-schrift'
        # Find team blocks in the HTML using specified tag and class
        team_blocks = soup.find_all(tag_name, class_=attribute_name)

        # Check if team blocks are found in the HTML
        if not team_blocks:
            LOGGER.error(f'Block teams with current tag name "{tag_name}" and '
                         f'attribute "{attribute_name}" in HTML code not found.')
            return

        for team_block in team_blocks:
            #
            home_block = team_block.find('td', class_='rechts hauptlink no-border-rechts hide-for-small '
                                                      'spieltagsansicht-vereinsname')
            away_block = team_block.find('td', class_='hauptlink no-border-links no-border-rechts hide-for-small '
                                                      'spieltagsansicht-vereinsname')

            home_team = home_block.find('a').get('title')
            away_team = away_block.find('a').get('title')

            # We handle the case where an additional information block is present on the page for some matches
            if home_team == 'Go to matchday thread':
                home_team = home_block.find_next_sibling().find_next_sibling().find('a').get('title')
            if away_team == 'Go to matchday thread':
                away_team = away_block.find_next_sibling().find_next_sibling().find('a').get('title')

            # We obtain a unique list of keys (home_team, away_team)
            keys.append(f'{self.format_string(home_team)}-vs-{self.format_string(away_team)}')
        return keys

    @staticmethod
    def create_values(soup: BeautifulSoup):
        """
        Create values for the mapping dictionary.

        Args:
            soup (BeautifulSoup): BeautifulSoup object representing the HTML content.

        Returns:
            List: List of values for the mapping dictionary.
        """
        return [f"https://www.transfermarkt.com{match.get('href')}" for match in soup.find_all('a', class_='liveLink')]

    def start_mapping(self, matchday: int) -> Optional[dict]:
        """
        Start the mapping process for a given matchday.

        Args:
            matchday (int): The matchday for which mapping is performed.

        Returns:
            Optional[dict]: Dictionary containing the mapping results or None if an error occurs.
        """
        competition_id = self.part_url[self.competition][0]
        country_id = self.part_url[self.competition][1]

        # Construct the URL for a specific matchday in the competition
        url = (f'https://www.transfermarkt.com/{competition_id}'
               f'/spieltag/wettbewerb/{country_id}/plus/?saison_id={self.year}&spieltag={matchday}')

        html_content = self.get_html(url)
        if not html_content:
            LOGGER.warning(f'Failed to retrieve the element code from the provided link "{url}".')
            return

        soup = BeautifulSoup(html_content, 'html.parser')
        keys = self.create_keys(soup)
        values = self.create_values(soup)

        LOGGER.info(f'Keys and values were successfully obtained for the '
                    f'competition {self.competition} on matchday {matchday}.')

        # Combine keys and values into a dictionary
        return dict(zip(keys, values))


def main(competition: str):
    # From the JSON metadata file, we extract the
    # current year (season) of the competition and the number of matchdays
    tournaments_metadata = JsonHelper()
    tournaments_metadata.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, METADATA_FILE_NAME))

    year = tournaments_metadata.get(competition, 'year')
    total_matchday = (tournaments_metadata.get(competition, 'teams') - 1) * 2

    # A dictionary that contains a pair where the key
    # is a substring of the link and the value is the link to that match in another source
    transfermarkt_urls = {}
    link_mapper = LinkMapper(competition, year)

    with ThreadPoolExecutor() as executor:
        results = executor.map(link_mapper.start_mapping, range(1, total_matchday + 1))

        if results:
            for result in results:
                transfermarkt_urls.update(result)

    # Write the entire map into each competition's JSON file
    competition_urls = JsonHelper()
    competition_urls.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, f'{competition}_urls.json'))
    competition_urls.append(competition, 'transfermarkt_urls', transfermarkt_urls)
    competition_urls.write()

    LOGGER.info(f'For the competition "{competition}", "{len(transfermarkt_urls)}" links were obtained and added.')
