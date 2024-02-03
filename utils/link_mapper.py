from scripts.constants import (LINK_MAPPER_FILE_LOG, LEAGUES, TRANSFERMARKT_SUFFIX_URL,
                               PROJECT_DIRECTORY, RESOURCE_CATALOG, COMPETITIONS_JSON)
from concurrent.futures import ThreadPoolExecutor
from utils.logger import configure_logger
from utils.json_helper import JsonHelper
from typing import Optional, List, Dict
from scripts.fetcher import Fetcher
from bs4 import BeautifulSoup
import os

# Define constants for logging and directory structure
LOGGER = configure_logger(__name__, LINK_MAPPER_FILE_LOG)


class LinkMapper(Fetcher):
    def __init__(self, competition: str, year: int):
        super().__init__()
        self.competition = competition
        self.year = year

        self.suffix_pattern = dict(zip(LEAGUES, TRANSFERMARKT_SUFFIX_URL))
        self.matchday_count = 0
        self.key_count = 0

    @staticmethod
    def format_string(team_name: str) -> str:
        """
        Formats the team name string according to the specified template.
        """
        # Handle special character replacements
        special_replacements = {'&': 'and', '.': '-', 'í': 'i', 'á': 'a', 'é': 'e', 'î': 'i', 'ü': 'u'}
        for char, replacement in special_replacements.items():
            team_name = team_name.replace(char, replacement)

        # Convert to lowercase and replace spaces with hyphens
        team_name = team_name.lower().replace(' ', '-')

        return team_name

    def create_keys(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """
        Create keys for the mapping dictionary.
        """
        keys = []

        tag_name = 'tr'
        attribute_name = 'table-grosse-schrift'
        # Find team blocks in the HTML using specified tag and class
        team_blocks = soup.find_all(tag_name, class_=attribute_name)

        # Check if team blocks are found in the HTML
        if not team_blocks:
            LOGGER.error(f'Block teams with current tag name "{tag_name}" and '
                         f'attribute "{attribute_name}" in HTML code not found.')
            return None

        for team_block in team_blocks:
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
    def create_values(soup: BeautifulSoup) -> Optional[List[str]]:
        """
        Create values for the mapping dictionary.
        """
        try:
            # Parsing links to matches that have already been played
            live_links = [f'https://www.transfermarkt.com{match.get("href")}' for match in
                          soup.find_all('a', class_='liveLink')]
            # Parsing links to matches that have not yet been played
            preview_links = [f'https://www.transfermarkt.com{match.get("href")}' for match in
                             soup.find_all('a', title='Match preview')]

            return live_links + preview_links
        except AttributeError:
            LOGGER.warning(f'AttributeError occurred while parsing match links. The list of values is not received.')
            return None

    def start_mapping(self, matchday: int) -> Optional[Dict[str, str]]:
        competition_id = self.suffix_pattern[self.competition][0]
        country_id = self.suffix_pattern[self.competition][1]

        # Construct the URL for a specific matchday in the competition
        url = (f'https://www.transfermarkt.com/{competition_id}'
               f'/spieltag/wettbewerb/{country_id}/plus/?saison_id={self.year}&spieltag={matchday}')

        try:
            html_content = self.fetch_data(url)
            if not html_content:
                LOGGER.warning(f'Failed to retrieve the element code from the provided link "{url}".')
                return None

        except Exception as e:
            LOGGER.warning(str(e).strip())
            return None

        soup = BeautifulSoup(html_content, 'html.parser')
        keys = self.create_keys(soup)
        values = self.create_values(soup)

        if len(keys) == len(values):
            self.matchday_count += 1
            self.key_count += len(keys)
            return dict(zip(keys, values))

        LOGGER.warning(f'The current page "{url}" received a different number of elements for keys and values.')
        return None


def main(competition: str):
    # From the JSON metadata file, we extract the
    # current year (season) of the competition and the number of matchdays
    competitions_json = JsonHelper()
    competitions_json.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, COMPETITIONS_JSON))

    year = competitions_json.get(competition, 'year')
    if competition == 'kz_premier_league':
        # This is how the link to Transfermarkt is structured
        year -= 1
    total_matchday = (competitions_json.get(competition, 'teams') - 1) * 2

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
    competition_urls.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, f'{competition}.json'))
    competition_urls.append(competition, 'transfermarkt', transfermarkt_urls)
    competition_urls.write()

    LOGGER.info(f'For the competition "{competition}", '
                f'successfully processed {link_mapper.matchday_count} rows from the Transfermarkt source.')
    LOGGER.info(f'For the competition "{competition}", "{link_mapper.key_count}" links were obtained and added.')
