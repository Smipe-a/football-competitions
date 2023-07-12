import re
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

dict_match_url = {}


def format_string(team_name: str) -> str:
    return re.sub(r'( FC|&|AFC )', '', team_name).lower().replace(' ', '-')


for number_page in range(1, 39):
    url = f'https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/plus/?saison_id=2022&spieltag=' \
          f'{number_page}'
    response = requests.get(url, headers={'User-Agent': UserAgent().chrome}, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    list_teams = []
    team_blocks = soup.find_all('tr', class_='table-grosse-schrift')
    for team_block in team_blocks:
        home_block = team_block.find('td', class_='rechts hauptlink no-border-rechts hide-for-small '
                                                  'spieltagsansicht-vereinsname')
        home_team = format_string(home_block.find('a').get('title'))

        away_block = team_block.find('td', class_='hauptlink no-border-links no-border-rechts hide-for-small '
                                                  'spieltagsansicht-vereinsname')
        away_team = format_string(away_block.find('a').get('title'))

        list_teams.append(f'{home_team}-vs-{away_team}')

    list_url_match = [f"https://www.transfermarkt.com{match.get('href')}" for match in
                      soup.find_all('a', class_='liveLink')]

    dict_match_url.update(zip(list_teams, list_url_match))
