from utils.db_connector import connect_to_database
from concurrent.futures import ThreadPoolExecutor
from utils.logger import configure_logger
from scripts.fetcher import Fetcher
from typing import Optional, List, Any
from bs4 import BeautifulSoup
from psycopg2 import errors
import concurrent.futures
import math


LOGGER = configure_logger(__name__, 'players.log')

class PlayersParser(Fetcher):
    def __init__(self):
        super().__init__()
        self.insert_iteration = 0
        self.futgg_url = 'https://www.fut.gg/players/'
        # self.url_card = 'https://www.fut.gg/api/fut/player-item-definitions/'
        self.futgg_json = 'https://www.fut.gg/api/fut/players/?page='

    def get_last_page(self, html_content: str) -> Optional[int]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            last_page_href = soup.find('div', 
                                       class_='pagination__control pagination__control--next').find('a').get('href')
            last_page = int(last_page_href.split('=')[1])
            return last_page
        
        except Exception as e:
            LOGGER.error(f'Error parsing last page number: {e}')
            return None

    def calculate_total_cards(self, html_content: str, last_page_number: int) -> Optional[int]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            cards_on_last_page = len(soup.find_all('div', class_='-my-1'))
            
            all_cards = 20 * (last_page_number - 1) + cards_on_last_page
            return all_cards
        except Exception as e:
            LOGGER.error(f'Error counting cards on the last page: {e}')
            return None

    def get_total_pages(self) -> int:
        html_content = self.fetch_data(self.futgg_url)
        if not html_content:
            LOGGER.error('Failed to retrieve the initial HTML content.')
            return 0

        last_page_number = self.get_last_page(html_content)
        if last_page_number is None:
            LOGGER.error('Failed to determine the last page number.')
            return 0

        html_content = self.fetch_data(f'{self.futgg_url}?page={last_page_number}')
        if not html_content:
            LOGGER.error(f'Failed to retrieve the HTML content for the last page: {last_page_number}.')
            return 0
        
        total_cards = self.calculate_total_cards(html_content, last_page_number)
        if total_cards is None:
            LOGGER.error('Failed to determine the total number of cards.')
            return 0

        return math.ceil(total_cards / 30)

    def insert_to_database(self, values: List[List], cursor, connection) -> None:
        insert_query = f"""
                        INSERT INTO eafc24.players VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) ON CONFLICT DO NOTHING;
                        """
        cursor.executemany(insert_query, values)
        connection.commit()

    def parse_page(self, page: int) -> List[List[Any]]:
        self.insert_iteration += 1
        print(self.insert_iteration)
        json_content = self.fetch_data(f'{self.futgg_json}{page}', 'json')
        if not json_content:
            LOGGER.warning(f'Failed to retrieve the element code from the provided link "{self.futgg_json}{page}".')
            return []

        package_eaid = []
        for card in json_content['data']:
            if card['club'] is not None:
                club_name = card['club']['name']
            else:
                club_name = None
            
            package_eaid.append([
                card['eaId'],
                card['firstName'],
                card['lastName'],
                card['league']['name'],
                club_name,
                card['nation']['name'],
                card['rarityName'],
                card['position'],
                card['foot']
            ])
        return package_eaid

    def get_basic_info(self) -> None:
        total_pages = self.get_total_pages()

        with connect_to_database() as connection, connection.cursor() as cursor:
            with ThreadPoolExecutor() as executor:
                future_to_page = {executor.submit(self.parse_page, page): page for page in range(1, total_pages + 1)}

                for future in concurrent.futures.as_completed(future_to_page):
                    package_eaid = future.result()
                    if package_eaid:
                        self.insert_to_database(package_eaid, cursor, connection)
            

players_parser = PlayersParser()
players_parser.get_basic_info()
