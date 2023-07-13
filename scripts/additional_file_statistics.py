# For educational purposes, a method for bypassing a 403 error
# in a GET request was applied using the fake_useragent module.
# During the execution of the request, only data about Premier League
# players was obtained, and no other information was extracted.
# All the acquired data was solely taken from the transfermarkt.com website
import re
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor


# An example of string formatting:
# AFC Bournemouth -> bournemouth
def format_string(team_name: str) -> str:
    return re.sub(r'( FC|AFC )', '', team_name).replace('&', 'and').lower().replace(' ', '-')


def get_teams_urls(soup) -> list:
    # Parse the home (left) and away (right) teams
    teams_urls = []
    team_blocks = soup.find_all('tr', class_='table-grosse-schrift')
    for team_block in team_blocks:
        home_block = team_block.find('td', class_='rechts hauptlink no-border-rechts hide-for-small '
                                                  'spieltagsansicht-vereinsname')
        away_block = team_block.find('td', class_='hauptlink no-border-links no-border-rechts hide-for-small '
                                                  'spieltagsansicht-vereinsname')

        home_team = home_block.find('a').get('title')
        away_team = away_block.find('a').get('title')

        if home_team == 'Go to matchday thread':
            home_team = home_block.find_next_sibling().find_next_sibling().find('a').get('title')
        if away_team == 'Go to matchday thread':
            away_team = away_block.find_next_sibling().find_next_sibling().find('a').get('title')

        teams_urls.append(f'{format_string(home_team)}-vs-{format_string(away_team)}')
    return teams_urls


def get_match_urls(soup) -> list:
    # Parse the match url
    return [f"https://www.transfermarkt.com{match.get('href')}" for match in soup.find_all('a', class_='liveLink')]


def parse_match_urls(number_page: int) -> dict:
    # Parse each page, where a page represents a matchday in the Premier League
    url = f'https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/plus/?saison_id=2022&spieltag=' \
          f'{number_page}'
    response = requests.get(url, headers={'User-Agent': UserAgent().chrome}, timeout=20)
    soup = BeautifulSoup(response.text, 'html.parser')

    teams_urls = get_teams_urls(soup)
    match_urls = get_match_urls(soup)
    return dict(zip(teams_urls, match_urls))


def parse_transfermarkt_matches() -> dict:
    # A dictionary that contains a pair where the key
    # is a substring of the link and the value is the link to that match in another source
    dict_match_url = {}

    with ThreadPoolExecutor() as executor:
        results = executor.map(parse_match_urls, range(1, 39))

        for result in results:
            dict_match_url.update(result)
    return dict_match_url


dict_match_all_url = parse_transfermarkt_matches()
