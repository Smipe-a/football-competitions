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
from additional_file_statistics import dict_match_all_url

# Create a list that contains a pair of teams for the current match_id and detailed match statistics
players_statistics_home_team = []
players_statistics_away_team = []
match_officials = []


def quantity_events(event):
    # We count the number of occurrences of a specific event
    count_events = 0
    for _ in event:
        count_events += 1
    return count_events


def format_minutes_to_int(minute) -> int:
    minute = re.sub(r'[^\d+]', '', minute)
    if minute.find('+') == -1:
        return int(minute)
    else:
        index_plus = minute.find('+')
        return int(minute[:index_plus]) + int(minute[index_plus + 1:])


def format_age(age: str) -> int:
    return int(re.sub(r'\D', '', age))


def format_position(position: str) -> str:
    if position == 'Goalkeeper':
        return 'GK'
    abbr_position = position.replace('-', ' ').split(' ')
    if len(abbr_position) < 2:
        return abbr_position[0][0].upper() + abbr_position[0][1:]
    return abbr_position[0][0] + abbr_position[1][0]


def format_price(price: str):
    coefficient = 1
    if price[-1] == 'm':
        coefficient = 1e6
    elif price[-1] == 'k':
        coefficient = 1e3

    match = re.search(r'[\d.]+', price)
    if match:
        return float(match.group()) * coefficient
    return None


def get_additional_info(url, player_skysports, dict_players, block_players):
    sub_block_players = block_players.find_all('table', class_='inline-table')
    i = 0
    for player in sub_block_players:
        player_transfermarket = block_players.select('td.zentriert.rueckennummer')[i].find(
            'div', class_='rn_nummer').text.strip()

        # Fix two number players
        if url == f'https://www.transfermarkt.com/manchester-city_fulham-fc/aufstellung/spielbericht/3838230' and \
                format_price(player.find('tr').find_next_sibling().text.strip()) == 1000000.0 and \
                format_age(player.find('tr').text.strip()) == 17:
            player_transfermarket = '82'

        if url == 'https://www.transfermarkt.com/leicester-city_brentford-fc/aufstellung/spielbericht/3837818' and \
                player_transfermarket == '12':
            player_transfermarket = '1'
        if url == 'https://www.transfermarkt.com/leicester-city_brentford-fc/aufstellung/spielbericht/3837818' and \
                player_transfermarket == '29' and \
                format_price(player.find('tr').find_next_sibling().text.strip()) == 20000000.0:
            player_transfermarket = '20'

        if str(player_skysports) == player_transfermarket:
            nationality = player.find_next('td', class_='zentriert').find('img').get('title')
            age = format_age(player.find('tr').text.strip())
            position = format_position(block_players.select('td.zentriert.rueckennummer')[i].get('title'))
            transfer_fee = format_price(player.find('tr').find_next_sibling().text.strip())

            dict_players[player_transfermarket] = [nationality, age, position, transfer_fee]
        i += 1


def get_statistics(url, players, soup_transfermarket, is_home, is_first_team):
    for player in players:
        number = player.find_previous('dt', class_='sdc-site-team-lineup__player-number').text.strip()
        # Find the first name and last name of the football player,
        # if the first name is not specified, we keep only the last name
        try:
            name = player.find('span', class_='sdc-site-team-lineup__player-initial').get('title')
        except AttributeError:
            name = ''
        surname = player.find('span', class_='sdc-site-team-lineup__player-surname').text.strip()
        if name == '':
            full_name = surname
        else:
            full_name = f'{name} {surname}'

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
                        play_time = format_minutes_to_int(sub_off_time)
                else:
                    if event.find(class_='sdc-site-team-lineup__item-event-icon--sub-on'):
                        sub_on = event.find_all('li', class_='sdc-site-team-lineup__event')[0]
                        sub_on_time = sub_on.find('span', class_='sdc-site-team-lineup__visually-hidden').text.strip()
                        substitution = True
                        play_time = 90 - format_minutes_to_int(sub_on_time)
                    if event.find(class_='sdc-site-team-lineup__item-event-icon--sub-off'):
                        sub_off = event.find_all('li', class_='sdc-site-team-lineup__event')[1]
                        sub_off_time = sub_off.find('span', class_='sdc-site-team-lineup__visually-hidden').text.strip()
                        play_time = 90 - format_minutes_to_int(sub_off_time) - play_time

                # Counting the number of events that have occurred
                goals = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--goal'))
                penalty_goals = quantity_events(
                    event.find_all(class_='sdc-site-team-lineup__item-event-icon--penalty'))
                missed_penalty_goals = quantity_events(
                    event.find_all(class_='sdc-site-team-lineup__item-event-icon--penalty-miss'))
                own_goals = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--own-goal'))
                assists = quantity_events(event.find_all(class_='sdc-site-team-lineup__item-event-icon--assist'))

                if event.find_all(class_='sdc-site-team-lineup__item-event-icon--yellow-card'):
                    yellow_card = True
                if event.find_all(class_='sdc-site-team-lineup__item-event-icon--red-card'):
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

        # print(dict_info_players)

        print(number, full_name, dict_info_players[str(number)][0], dict_info_players[str(number)][1],
              dict_info_players[str(number)][2], dict_info_players[str(number)][3], is_first_team)

        player_statistics = {
            'match_id': int(url[-7:]),
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
        # print(player_statistics)

        # Check if the player played for the away team or the home team
        if is_home:
            players_statistics_home_team.append(player_statistics)
        else:
            players_statistics_away_team.append(player_statistics)


def set_officials_match(match_id: int, name_officials: str, role: str):
    official = {
        'match_id': match_id,
        'name_officials': name_officials,
        'role': role
    }
    match_officials.append(official)


# Parsing referee crew data from the skysports website
# and adding missing coach information for each match from the transfermarket website
def get_officials_match(source_url, soup_skysports, soup_transfermarket):
    match_id = int(source_url[-7:])

    manager_block = soup_transfermarket.find_all('div', class_='row sb-formation')[-1]
    manager_name_block = manager_block.find_all('table', class_='inline-table')
    manager_home_name = manager_name_block[0].find('img').get('title')
    manager_away_name = manager_name_block[1].find('img').get('title')

    set_officials_match(match_id, manager_home_name, 'Manager Home Team')
    set_officials_match(match_id, manager_away_name, 'Manager Away Team')

    officials_list = soup_skysports.find('dl', class_='sdc-site-team-lineup__officials-list')
    block_officials = officials_list.find_all('dd', class_='sdc-site-team-lineup__officials-name')
    officials_data = [(official.text.strip().split(', '), official.get('data-officials-role')) for official in
                      block_officials]
    for name_officials, officials_role in officials_data:
        for name_person in name_officials:
            set_officials_match(match_id, name_person, officials_role)


# Open the file that contains URLs of all matches
def scrape_data():
    i = 1
    with open('../urls/file_players_url.txt', 'r') as file:
        for url in file:
            print(i)
            i += 1

            response = requests.get(url.strip(), timeout=20)
            soup_ss = BeautifulSoup(response.text, 'html.parser')

            transfermarkt_match_sheet = dict_match_all_url[url[35:-14]]
            response = requests.get(transfermarkt_match_sheet, headers={'User-Agent': UserAgent().chrome}, timeout=50)
            print(transfermarkt_match_sheet)

            # Generating a link to navigate to the page with match lineups
            url_lineup = BeautifulSoup(response.text, 'html.parser').find('a', string='Line-ups').get('href')
            transfermarket_line_ups = f'https://www.transfermarkt.com{url_lineup}'

            response = requests.get(transfermarket_line_ups, headers={'User-Agent': UserAgent().chrome}, timeout=20)
            soup_tm = BeautifulSoup(response.text, 'html.parser')

            # Creating the match_officials table
            get_officials_match(url, soup_ss, soup_tm)

            # Creating the players_statistics tables
            block_team = soup_ss.find_all('div', class_='sdc-site-team-lineup__header '
                                                        'sdc-site-team-lineup__header--main')
            flag_team = True
            for team in block_team:
                main_players = team.find_next_sibling('dl').find_all('dd', class_='sdc-site-team-lineup__player-name')
                get_statistics(transfermarket_line_ups, main_players, soup_tm, flag_team, True)

                substitutes = team.find_next_sibling('h5', class_='sdc-site-team-lineup__header--subs')
                if substitutes:
                    substitutes_players = substitutes.find_next_sibling('dl').find_all(
                        'dd', class_='sdc-site-team-lineup__player-name')
                    get_statistics(transfermarket_line_ups, substitutes_players, soup_tm, flag_team, False)
                flag_team = False


if __name__ == '__main__':
    scrape_data()
    data_match_officials = pd.DataFrame(match_officials)
    data_match_officials = data_match_officials.set_index('match_id')
    data_match_officials.to_csv('../data/match_officials.csv')

    data_players_statistics_home_team = pd.DataFrame(players_statistics_home_team)
    data_players_statistics_home_team = data_players_statistics_home_team.set_index('match_id')
    data_players_statistics_home_team.to_csv('../data/players_statistics_home_team.csv')

    data_players_statistics_away_team = pd.DataFrame(players_statistics_away_team)
    data_players_statistics_away_team = data_players_statistics_away_team.set_index('match_id')
    data_players_statistics_away_team.to_csv('../data/players_statistics_away_team.csv')
