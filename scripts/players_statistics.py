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


match_officials = []
# Create a list that contains a pair of teams for the current match_id and detailed match statistics
players_statistics_home_team = []
players_statistics_away_team = []


# We count the number of occurrences of a specific event
def quantity_events(event) -> int:
    return len(event)


# Example formatting for minutes (also formats a string into age):
# 90'+3' = 93
def format_to_int(value: str) -> int:
    return sum([int(num) for num in re.findall(r'\d+', value)])


def format_position(position: str) -> str:
    if position == 'Goalkeeper':
        return 'GK'
    abbreviation = ''.join(word[0].upper() for word in position.replace('-', ' ').split(' '))
    return abbreviation


def format_price(price: str):
    coefficient_mapping = {'m': 1e6, 'k': 1e3}
    coefficient = coefficient_mapping.get(price[-1], 1)

    match = re.search(r'[\d.]+', price)
    if match:
        return float(match.group()) * coefficient
    return None


# We supplement the main information from the skysports
# source with additional information from transfermarket (transfer value, age, nationality, position)
def get_additional_info(url: str, number_in_skysports, dict_players: dict, block_players):
    sub_block_players = block_players.find_all('table', class_='inline-table')
    for idx_block, player in enumerate(sub_block_players):
        number_in_transfermarket = block_players.select('td.zentriert.rueckennummer')[idx_block].find(
            'div', class_='rn_nummer').text.strip()

        # Fix two number players (80 and 80)
        if url == f'https://www.transfermarkt.com/manchester-city_fulham-fc/aufstellung/spielbericht/3838230' and \
                format_price(player.find('tr').find_next_sibling().text.strip()) == 1000000.0 and \
                format_to_int(player.find('tr').text.strip()) == 17:
            number_in_transfermarket = '82'

        # We are correcting the error when player numbers differ between two different websites
        if url == f'https://www.transfermarkt.com/leicester-city_brentford-fc/aufstellung/spielbericht/3837818' and \
                number_in_transfermarket == '12':
            number_in_transfermarket = '1'

        if url == f'https://www.transfermarkt.com/leicester-city_brentford-fc/aufstellung/spielbericht/3837818' and \
                number_in_transfermarket == '29' and \
                format_price(player.find('tr').find_next_sibling().text.strip()) == 20000000.0:
            number_in_transfermarket = '20'

        if str(number_in_skysports) == number_in_transfermarket:
            nationality = player.find_next('td', class_='zentriert').find('img').get('title')
            age = format_to_int(player.find('tr').text.strip())
            position = format_position(block_players.select('td.zentriert.rueckennummer')[idx_block].get('title'))
            transfer_fee = format_price(player.find('tr').find_next_sibling().text.strip())

            dict_players[number_in_transfermarket] = [nationality, age, position, transfer_fee]


# Obtain the basic statistics of players per match
def get_statistics(match_id: int, url: str, players, soup_transfermarket, is_home: bool, is_first_team: bool):
    for player in players:
        # Find the first name and last name of the football player,
        # if the first name is not specified, we keep only the last name
        number = player.find_previous('dt', class_='sdc-site-team-lineup__player-number').text.strip()
        try:
            name = player.find('span', class_='sdc-site-team-lineup__player-initial').get('title')
        except AttributeError:
            name = ''
        surname = player.find('span', class_='sdc-site-team-lineup__player-surname').text.strip()
        full_name = surname if not name else f'{name} {surname}'

        if is_first_team:
            play_time = 90
        else:
            play_time = 0
        goals, penalty_goals, missed_penalty_goals, own_goals, assists = 0, 0, 0, 0, 0
        substitution, yellow_card, red_card = False, False, False

        event_list = player.find_all(class_='sdc-site-team-lineup__events')
        try:
            for event in event_list:
                if is_first_team:
                    if event.find(class_='sdc-site-team-lineup__item-event-icon--sub-off'):
                        sub_off = event.find_all('li', class_='sdc-site-team-lineup__event')[-1]
                        sub_off_time = sub_off.find('span', class_='sdc-site-team-lineup__visually-hidden').text.strip()
                        substitution = True
                        play_time = format_to_int(sub_off_time)
                else:
                    if event.find(class_='sdc-site-team-lineup__item-event-icon--sub-on'):
                        sub_on = event.find_all('li', class_='sdc-site-team-lineup__event')[0]
                        sub_on_time = sub_on.find('span', class_='sdc-site-team-lineup__visually-hidden').text.strip()
                        substitution = True
                        play_time = abs(90 - format_to_int(sub_on_time))
                    if event.find(class_='sdc-site-team-lineup__item-event-icon--sub-off'):
                        sub_off = event.find_all('li', class_='sdc-site-team-lineup__event')[1]
                        sub_off_time = sub_off.find('span', class_='sdc-site-team-lineup__visually-hidden').text.strip()
                        play_time = abs(90 - format_to_int(sub_off_time)) - play_time

                # Counting the number of events that have occurred
                goals = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--goal'))
                penalty_goals = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--penalty'))
                missed_penalty_goals = quantity_events(
                    event.find_all(class_='sdc-site-team-lineup__item-event-icon--penalty-miss'))
                own_goals = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--own-goal'))
                assists = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--assist'))

                if event.find_all(class_='sdc-site-team-lineup__item-event-icon--yellow-card'):
                    yellow_card = True
                if event.find_all(class_='sdc-site-team-lineup__item-event-icon--red-card'):
                    block_red_card = event.find_all(class_='sdc-site-team-lineup__item-event-icon--red-card')
                    block_minute = block_red_card[0].find_next(class_="sdc-site-team-lineup__event_time").text.strip()
                    if is_first_team:
                        play_time = format_to_int(block_minute)
                    else:
                        sub_on = event.find_all('li', class_='sdc-site-team-lineup__event')[0]
                        sub_on_time = sub_on.find('span', class_='sdc-site-team-lineup__visually-hidden').text.strip()
                        play_time = format_to_int(block_minute) - format_to_int(sub_on_time)

                    red_card = True
        except AttributeError:
            if is_first_team:
                play_time = 90
            else:
                play_time = 0
            goals, penalty_goals, missed_penalty_goals, own_goals, assists = 0, 0, 0, 0, 0
            substitution, yellow_card, red_card = False, False, False

        # Supplement the information from transfermarket.com
        # with transfer fee, age, nationality, and position at the time of the match
        all_home_players = soup_transfermarket.find_all('div', class_='responsive-table')
        # Create a dictionary where the key is the player's number,
        # and the value is a list with a dimension of 4
        dict_info_players = {}

        if is_home:
            get_additional_info(url, number, dict_info_players, all_home_players[0])
            get_additional_info(url, number, dict_info_players, all_home_players[2])
        else:
            get_additional_info(url, number, dict_info_players, all_home_players[1])
            get_additional_info(url, number, dict_info_players, all_home_players[3])

        player_statistics = {
            'match_id': match_id,
            'number_player': number,
            'name_player': full_name,
            'nationality': dict_info_players[str(number)][0],
            'age': dict_info_players[str(number)][1],
            'position': dict_info_players[str(number)][2],
            'transfer_fee': dict_info_players[str(number)][3],
            'first_team': is_first_team,
            'substitution': substitution,
            'play_time': play_time,
            'goals': goals,
            'penalty_goals': penalty_goals,
            'missed_penalty_goals': missed_penalty_goals,
            'own_goals': own_goals,
            'assists': assists,
            'yellow_card': yellow_card,
            'red_card': red_card
        }

        # Check if the player played for the away team or the home team
        if is_home:
            players_statistics_home_team.append(player_statistics)
        else:
            players_statistics_away_team.append(player_statistics)


def set_officials_match(match_id: int, name_officials: str, role: str) -> dict:
    return {
        'match_id': match_id,
        'name_officials': name_officials,
        'role': role
    }


# Parsing referee crew data from the skysports website
# and adding missing coach information for each match from the transfermarket website
def get_officials_match(match_id: int, soup_skysports, soup_transfermarket) -> list:
    officials_match = []

    manager_block = soup_transfermarket.find_all('div', class_='row sb-formation')[-1]
    manager_name_block = manager_block.find_all('table', class_='inline-table')
    manager_home_name = manager_name_block[0].find('img').get('title')
    manager_away_name = manager_name_block[1].find('img').get('title')

    officials_match.append(set_officials_match(match_id, manager_home_name, 'Manager Home Team'))
    officials_match.append(set_officials_match(match_id, manager_away_name, 'Manager Away Team'))

    officials_list = soup_skysports.find('dl', class_='sdc-site-team-lineup__officials-list')
    block_officials = officials_list.find_all('dd', class_='sdc-site-team-lineup__officials-name')
    officials_data = [(official.text.strip().split(', '), official.get('data-officials-role')) for official in
                      block_officials]
    for name_officials, officials_role in officials_data:
        for name_person in name_officials:
            officials_match.append(set_officials_match(match_id, name_person, officials_role))
    return officials_match


# Open the file that contains URLs of all matches
def scrape_data(url: str):
    response = requests.get(url.strip(), timeout=20)
    soup_ss = BeautifulSoup(response.text, 'html.parser')

    transfermarkt_match_sheet = dict_match_all_url[url[35:-14]]
    response = requests.get(transfermarkt_match_sheet, headers={'User-Agent': UserAgent().chrome}, timeout=50)

    # Generating a link to navigate to the page with match lineups
    url_lineup = BeautifulSoup(response.text, 'html.parser').find('a', string='Line-ups').get('href')
    transfermarket_line_ups = f'https://www.transfermarkt.com{url_lineup}'

    response = requests.get(transfermarket_line_ups, headers={'User-Agent': UserAgent().chrome}, timeout=20)
    soup_tm = BeautifulSoup(response.text, 'html.parser')

    # Creating the match_officials table
    match_officials.extend(get_officials_match(int(url[-7:]), soup_ss, soup_tm))

    # Creating the players_statistics tables
    block_team = soup_ss.find_all('div', class_='sdc-site-team-lineup__header sdc-site-team-lineup__header--main')
    flag_team = True
    for team in block_team:
        main_players = team.find_next_sibling('dl').find_all('dd', class_='sdc-site-team-lineup__player-name')
        get_statistics(int(url[-7:]), transfermarket_line_ups, main_players, soup_tm, flag_team, True)

        substitutes = team.find_next_sibling('h5', class_='sdc-site-team-lineup__header--subs')
        if substitutes:
            substitutes_players = substitutes.find_next_sibling('dl').find_all(
                'dd', class_='sdc-site-team-lineup__player-name')
            get_statistics(int(url[-7:]), transfermarket_line_ups, substitutes_players, soup_tm, flag_team, False)
        flag_team = False


if __name__ == '__main__':
    with open('../urls/file_players_url.txt', 'r') as file:
        urls = file.readlines()

    with ThreadPoolExecutor() as executor:
        for i in range(0, len(urls), 10):
            batch_urls = urls[i: i + 10]
            executor.map(scrape_data, batch_urls)

    data_match_officials = pd.DataFrame(match_officials).set_index('match_id')
    data_match_officials.to_csv('../data/match_officials.csv')

    data_players_statistics_home_team = pd.DataFrame(players_statistics_home_team).set_index('match_id')
    data_players_statistics_home_team.to_csv('../data/players_statistics_home_team.csv')

    data_players_statistics_away_team = pd.DataFrame(players_statistics_away_team).set_index('match_id')
    data_players_statistics_away_team.to_csv('../data/players_statistics_away_team.csv')
