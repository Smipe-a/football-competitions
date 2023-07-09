import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Create a list that contains a pair of teams for the current match_id and detailed match statistics
players_statistics_home_team = []
players_statistics_away_team = []
match_officials = []


def get_statistics(match_id, players, is_home, is_first_team):
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

        start_minute, substitution_minute = 0, 0
        goals, penalty_goals, missed_penalty_goals, own_goals, assists = 0, 0, 0, 0, 0
        substitution, yellow_card, red_card = False, False, False

        event_list = player.find_all(class_='sdc-site-team-lineup__events')
        try:
            for events in event_list:
                if is_first_team:
                    if events.find_all(class_='sdc-site-team-lineup__item-event-icon--sub-off'):
                        # event_sub_off = events.find_all(class_='sdc-site-team-lineup__item-event-icon--sub-off')
                        # event_off = event_sub_off.find_next(class_="sdc-site-team-lineup__event_time").text.strip()
                        # print()
                        # print(event_off)
                        substitution = True
                        #substitution_minute = format_minutes_to_int(event_off[0])
                else:

                    if events.find_all(class_='sdc-site-team-lineup__item-event-icon--sub-on'):
                        substitution = True
                        # event_sub_on = events.find_all(class_='sdc-site-team-lineup__item-event-icon--sub-on')
                        # event_on = event_sub_on.find_next(class_="sdc-site-team-lineup__event_time").text.strip()
                        # print()
                        # print(event_on)
                        # start_minute = format_minutes_to_int(event_on[0])
                        # event_sub_off = events.find_all(class_='sdc-site-team-lineup__item-event-icon--sub-off')
                        # if event_sub_off:
                        #     substitution_minute = format_minutes_to_int(event_sub_off[0])

                goals = quantity_events(events.find_all(class_='sdc-site-team-lineup__item-event-icon--goal'))
                penalty_goals = quantity_events(
                    events.find_all(class_='sdc-site-team-lineup__item-event-icon--penalty'))
                missed_penalty_goals = quantity_events(
                    events.find_all(class_='sdc-site-team-lineup__item-event-icon--penalty-miss'))
                own_goals = quantity_events(events.find_all(class_='sdc-site-team-lineup__item-event-icon--own-goal'))
                assists = quantity_events(events.find_all(class_='sdc-site-team-lineup__item-event-icon--assist'))

                if events.find_all(class_='sdc-site-team-lineup__item-event-icon--yellow-card'):
                    yellow_card = True
                if events.find_all(class_='sdc-site-team-lineup__item-event-icon--red-card'):
                    red_card = True
        except AttributeError:
            start_minute, substitution_minute = 0, 0
            goals, penalty_goals, missed_penalty_goals, own_goals, assists = 0, 0, 0, 0, 0
            substitution, yellow_card, red_card = False, False, False

        player_statistics = {
            'match_id': match_id,
            'number_player': number,
            'name_player': full_name,
            'first_team': is_first_team,
            'substitution': substitution,
            'start_minute': start_minute,
            'substitution_minute': substitution_minute,
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


def quantity_events(event):
    # We count the number of occurrences of a specific event
    count_events = 0
    for _ in event:
        count_events += 1
    return count_events


def format_minutes_to_int(minute):
    minute = re.sub(r'[^\d+]', '', minute)
    if minute.find('+') == -1:
        return int(minute)
    else:
        index_plus = minute.find('+')
        return int(minute[:index_plus]) + int(minute[index_plus + 1:])


# Open the file that contains URLs of all matches
file_path = os.path.abspath('../urls/file_players_url.txt')
i = 1
with open(file_path, 'r') as file:
    for url in file:
        print(i)
        i += 1
        response = requests.get(url.strip(), timeout=10)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        block_team = soup.find_all('div', class_='sdc-site-team-lineup__header sdc-site-team-lineup__header--main')
        flag_team = True

        for team in block_team:
            main_players = team.find_next_sibling('dl').find_all('dd', class_='sdc-site-team-lineup__player-name')
            get_statistics(int(url[-7:]), main_players, flag_team, True)

            substitutes = team.find_next_sibling('h5', class_='sdc-site-team-lineup__header--subs')
            if substitutes:
                substitutes_players = substitutes.find_next_sibling('dl').find_all(
                    'dd', class_='sdc-site-team-lineup__player-name')
                get_statistics(int(url[-7:]), substitutes_players, flag_team, False)
            flag_team = False

        block_officials = soup.find_all('div', class_='sdc-site-team-lineup__officials')
        for official in block_officials:
            officials = {
                'match_id': int(url[-7:]),
                'name_officials': official.find('dd', class_="sdc-site-team-lineup__officials-name").text.strip(),
                'role': official.find('dd', class_="sdc-site-team-lineup__officials-name").get('data-officials-role')
            }
            match_officials.append(officials)

data_players_statistics_home_team = pd.DataFrame(players_statistics_home_team)
data_players_statistics_away_team = pd.DataFrame(players_statistics_away_team)
data_match_officials = pd.DataFrame(match_officials)

data_players_statistics_home_team = data_players_statistics_home_team.set_index('match_id')
data_players_statistics_home_team.to_csv('../data/players_statistics_home_team.csv')

data_players_statistics_away_team = data_players_statistics_away_team.set_index('match_id')
data_players_statistics_away_team.to_csv('../data/players_statistics_away_team.csv')

data_match_officials = data_match_officials.set_index('match_id')
data_match_officials.to_csv('../data/match_officials.csv')
