from utils.db_connector import connect_to_database
from utils.logger import configure_logger
from bs4 import BeautifulSoup
from psycopg2 import sql
import requests
import sys

# Configure logger for the current module
LOGGER = configure_logger(__name__)


class StandingsParser:
    def __init__(self, competition: str):
        self.competition = competition

    def parse_standings(self, year_competition: str):
        url = f'https://www.skysports.com/{self.competition}-table/{year_competition}'

        try:
            response = requests.get(url)
            # Check
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', class_='standing-table__table')

            team_list = []

            for row in table.find_all('tr'):
                if len(cells := row.find_all('td')) > 0:
                    team_list.append(self.extract_data(cells, url))

            self.update_values(data_to_insert=team_list)

        except requests.exceptions.RequestException as req_err:
            LOGGER.error(req_err)
        except Exception as e:
            LOGGER.error(e)

    @staticmethod
    def extract_data(cells, url):
        return [
            cells[1].find('a', class_='standing-table__cell--name-link').get_text(strip=True).replace('*', ''),
            f'{url[-2:]}_{int(url[-2:]) + 1}',
            cells[0].get_text(strip=True),
            cells[3].get_text(strip=True),
            cells[4].get_text(strip=True),
            cells[5].get_text(strip=True),
            cells[6].get_text(strip=True),
            cells[7].get_text(strip=True),
            cells[9].get_text(strip=True)
        ]

    def update_values(self, data_to_insert):
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
            ).format(sql.Identifier(self.competition.replace('-', '_')))

            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(e)
        finally:
            cursor.close()
            connection.close()


class PremierLeagueParser(StandingsParser):
    def __init__(self):
        super().__init__('premier-league')


class LaLigaParser(StandingsParser):
    def __init__(self):
        super().__init__('la-liga')


def create_competition_parser(competition):
    class_name = ''.join(word.capitalize() for word in competition.split('-')) + 'Parser'
    return type(class_name, (StandingsParser,), {'__init__': lambda self: super().__init__(competition)})


if __name__ == '__main__':
    with open('../resources/gameweek_dates.txt', 'r') as file:
        # Format date start season: 2023-08-11 -> 2023
        year = file.readline()[:4]

    # if len(sys.argv) != 2:
    #     # WARNING
    #     sys.exit(1)
    #
    # competition_title = sys.argv[1]
    competition_parser = StandingsParser('premier-league')
    competition_parser.parse_standings(year_competition=year)
