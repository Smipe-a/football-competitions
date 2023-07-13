# For educational purposes, a method for bypassing a 403 error
# in a GET request was applied using the fake_useragent module.
# During the execution of the request, only data about Premier League
# players was obtained, and no other information was extracted.
# All the acquired data was solely taken from the transfermarkt.com website
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
from additional_file_statistics import dict_match_all_url


def get_stadium(header):
    # We check 'stadium' for the absence of a block in the HTML
    separator_venue = header.find(class_='sdc-site-match-header__detail-venue--with-seperator')
    if separator_venue:
        return separator_venue.text.strip()
    return header.find(class_='sdc-site-match-header__detail-venue').text.strip()


def get_attendance(header, url):
    # We check 'attendance' for the absence of a block in the HTML
    attendance_element = header.find('span', class_='sdc-site-match-header__detail-attendance')
    if attendance_element:
        attendance_text = attendance_element.text.split(':')[1].strip().replace(',', '')
        return int(re.findall(r'\d+', attendance_text)[0])
    else:
        transfermarkt_match_url = dict_match_all_url[url[35:-14]]
        response = requests.get(transfermarkt_match_url, headers={'User-Agent': UserAgent().chrome}, timeout=20)
        soup_tm = BeautifulSoup(response.text, 'html.parser')
        attendance_text = soup_tm.find('p', class_='sb-zusatzinfos').find('strong').text.split(': ')[1].replace('.', '')
        return int(attendance_text)


def extract_number(text):
    match = re.search(r'\d+', text)
    if match:
        return int(match.group())
    return None


def process_block_info(block_info):
    if block_info:
        return block_info.find(class_='sdc-site-match-stats__val').text.strip()
    return ''


def set_dict_statistics(team) -> dict:
    match_data_team = {
        'match_id': team[0],
        'stadium': team[1],
        'attendance': team[2],
        'is_home_team': team[3],
        'team': team[4],
        'possessions': team[5],
        'total_shots': team[6],
        'on_target': team[7],
        'off_target': team[8],
        'blocked': team[9],
        'passing': team[10],
        'clear_cut_chance': team[11],
        'corners': team[12],
        'offsides': team[13],
        'tackles': team[14],
        'aerial_duels': team[15],
        'saves': team[16],
        'fouls_committed': team[17],
        'fouls_won': team[18],
        'yellow_cards': team[19],
        'red_cards': team[20]
    }
    return match_data_team


def process_match(url):
    response = requests.get(url.strip(), timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    head = soup.find(class_='sdc-site-match-header__detail')

    stadium = get_stadium(head)
    attendance = get_attendance(head, url)

    # We set initial values that are the same for both teams
    # match_id, stadium, attendance, is_home_team
    if stadium[-1] == '.':
        stadium = stadium[:-1]
    if stadium == "St James' Park, Newcastle":
        stadium = "St. James' Park"

    home_team_statistics = [int(url[-7:]), stadium, attendance, True]
    away_team_statistics = [int(url[-7:]), stadium, attendance, False]

    body = soup.find(class_='sdc-site-match-header__content')
    home_team_statistics.append(body.find(class_='sdc-site-match-header__team-name').text.strip())
    away_team_statistics.append(body.find(class_='sdc-site-match-header__team-name--away').text.strip())

    # Finding the block in the HTML with the statistics and extracting it
    table_statistics = soup.find_all(class_='sdc-site-match-stats__stats')
    for block in table_statistics:
        block_info_home = block.find(class_='sdc-site-match-stats__stats-home')
        block_info_away = block.find(class_='sdc-site-match-stats__stats-away')

        home_team_statistics.append(process_block_info(block_info_home))
        away_team_statistics.append(process_block_info(block_info_away))
    return set_dict_statistics(home_team_statistics), set_dict_statistics(away_team_statistics)


def process_matches():
    # Create a list that contains a pair of teams for the current match_id and detailed match statistics
    match_statistics = []
    # Open the file that contains URLs of all matches
    with open('../urls/file_teams_url.txt', 'r') as file:
        urls = file.readlines()
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_match, urls)

        for match_data_home, match_data_away in results:
            match_statistics.extend([match_data_home, match_data_away])
    return match_statistics


def save_match_statistics(match_statistics):
    match_statistics_df = pd.DataFrame(match_statistics)
    match_statistics_df = match_statistics_df.set_index('match_id')
    match_statistics_df.to_csv('../data/match_statistics.csv')


if __name__ == '__main__':
    source_match_statistics = process_matches()
    save_match_statistics(source_match_statistics)
