import requests
import psycopg2
from decouple import config
from bs4 import BeautifulSoup


def parse_standings(url: str):
    # Send GET-request on web-site
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    # Find main table on site
    table = soup.find('table', class_='standing-table__table')

    # Create a list to receive data from page
    team_list = []
    for row in table.find_all('tr'):
        if row.find_all('td'):
            data_team = tuple([row.find_all('td')[1].get_text(strip=True)] + [row.find_all('td')[0].get_text(strip=True)])
            data_team += tuple(cell.get_text(strip=True) for cell in row.find_all('td')[2:-1])
            team_list.append(data_team)

    insert_data_to_db(season=f'season{url[-2:]}_{int(url[-2:]) + 1}', data_to_insert=team_list)


def insert_data_to_db(season: str, data_to_insert: list):
    database_params = {
        'dbname': 'english_premier_league',
        'user': config('PG_USER'),
        'password': config('PG_PASSWORD'),
        'host': config('PG_HOST'),
        'port': '5432'
    }
    connection = psycopg2.connect(**database_params)
    cursor = connection.cursor()

    insert_query = f"""
                   INSERT INTO {season}.standings (
                       team, position, played, won, drawn, lost, goals_for, goals_against, goals_difference, points
                   ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                   )
                   """

    cursor.executemany(insert_query, data_to_insert)
    connection.commit()
    cursor.close()


if __name__ == '__main__':
    with open('../resources/gameweek_dates.txt', 'r') as file:
        # Format date: 2023-08-11 -> 2023
        year = file.readline()[:4]

    parse_standings(url=f'https://www.skysports.com/premier-league-table/{year}')
