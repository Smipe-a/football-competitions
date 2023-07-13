import pandas as pd
import requests
from bs4 import BeautifulSoup


def parse_main_table(url: str) -> pd.DataFrame:
    # Send GET-request on web-site
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    # Find main table on site
    table = soup.find('table', class_='standing-table__table')

    # Create a list to receive data from page
    main_table_results = [
        {
            'position': cells[0].get_text(strip=True),
            'team': cells[1].find('a', class_='standing-table__cell--name-link').get_text(strip=True),
            'played': cells[2].get_text(strip=True),
            'won': cells[3].get_text(strip=True),
            'drawn': cells[4].get_text(strip=True),
            'lost': cells[5].get_text(strip=True),
            'goals_for': cells[6].get_text(strip=True),
            'goals_against': cells[7].get_text(strip=True),
            'goals_difference': cells[8].get_text(strip=True),
            'points': cells[9].get_text(strip=True)
        }
        # Iterate through each row of the table
        for row in table.find_all('tr')
        # Retrieve the value of a cell
        # Create a dictionary with the parsed data
        if len(cells := row.find_all('td')) > 0
    ]

    return pd.DataFrame(main_table_results).set_index('position')


if __name__ == '__main__':
    source_url = 'https://www.skysports.com/premier-league-table/2022'
    data_main_table_results = parse_main_table(source_url)
    data_main_table_results.to_csv('../data/main_table_results.csv')
