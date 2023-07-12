# For educational purposes, a method for bypassing a 403 error
# in a GET request was applied using the fake_useragent module.
# During the execution of the request, only data about Premier League
# players was obtained, and no other information was extracted.
# All the acquired data was solely taken from the transfermarkt.com website
import re
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# A dictionary that contains a pair where the key
# is a substring of the link and the value is the link to that match in another source
dict_match_url = {}


# An example of string formatting:
# AFC Bournemouth -> bournemouth
def format_string(team_name: str) -> str:
    return re.sub(r'( FC|&|AFC )', '', team_name).lower().replace(' ', '-')


# Parse each page, where a page represents a matchday in the Premier League
for number_page in range(1, 39):
    url = f'https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/plus/?saison_id=2022&spieltag=' \
          f'{number_page}'
    response = requests.get(url, headers={'User-Agent': UserAgent().chrome}, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Parse the home (left) and away (right) teams
    list_teams = [
        f'{format_string(home_block.find("a").get("title"))}-vs-{format_string(away_block.find("a").get("title"))}'
        for team_block in soup.find_all('tr', class_='table-grosse-schrift')
        for home_block, away_block in
        [(team_block.find('td', class_='rechts hauptlink no-border-rechts hide-for-small spieltagsansicht-vereinsname'),
          team_block.find('td', class_='hauptlink no-border-links no-border-rechts hide-for-small '
                                       'spieltagsansicht-vereinsname'))]
    ]

    # Parse the match url
    list_url_match = [f"https://www.transfermarkt.com{match.get('href')}" for match in
                      soup.find_all('a', class_='liveLink')]

    dict_match_url.update(zip(list_teams, list_url_match))
