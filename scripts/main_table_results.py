from db_connector import connect_to_database
from bs4 import BeautifulSoup
from psycopg2 import sql
import requests


def parse_standings(url: str):
    # Send GET-request on web-site
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    # Find main table on site
    table = soup.find('table', class_='standing-table__table')

    # Create a list to receive data from page
    team_list = []

    # Iterate through each row of the table
    # From 1 to 20
    for row in table.find_all('tr'):
        if len(cells := row.find_all('td')) > 0:
            team_list.append([
                cells[1].find('a', class_='standing-table__cell--name-link').get_text(strip=True).replace('*', ''),
                f'{url[-2:]}_{int(url[-2:]) + 1}',
                cells[0].get_text(strip=True),
                cells[3].get_text(strip=True),
                cells[4].get_text(strip=True),
                cells[5].get_text(strip=True),
                cells[6].get_text(strip=True),
                cells[7].get_text(strip=True),
                cells[9].get_text(strip=True)
            ])

    update_values(competition='premier_league', data_to_insert=team_list)


def update_values(competition: str, data_to_insert: list):
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
        ).format(sql.Identifier(competition))

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
    except Exception as e:
        connection.rollback()
        # Implementing the output of exceptions and messages to a log file
        print(e)
    finally:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    with open('../resources/gameweek_dates.txt', 'r') as file:
        # Format date start season: 2023-08-11 -> 2023
        year = file.readline()[:4]

        parse_standings(url=f'https://www.skysports.com/premier-league-table/{year}')
