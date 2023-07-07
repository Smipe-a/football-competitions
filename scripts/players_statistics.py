import os
import requests
import pandas as pd
from bs4 import BeautifulSoup


players_statistics_home_team = []
players_statistics_away_team = []


def first_function(match_id, players, team_flag):
    for player in players:

        number = player.find_previous('dt', class_='sdc-site-team-lineup__player-number').text.strip()
        try:
            name = player.find('span', class_='sdc-site-team-lineup__player-initial').get('title')
        except AttributeError:
            name = ""
        surname = player.find('span', class_='sdc-site-team-lineup__player-surname').text.strip()

        try:
            event_list = player.find_all(class_="sdc-site-team-lineup__events")
            goals = []
            penalty_goals = []
            not_penalty_goals = []
            own_goals = []
            substitutions = []
            assists = []
            yellow_cards = []
            red_cards = []
            for events in event_list:
                goal_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--goal")
                penalty_goal_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--penalty")
                not_penalty_goal_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--penalty-miss")
                own_goal_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--own-goal")
                substitution_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--sub-off")
                assist_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--assist")
                yellow_card_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--yellow-card")
                red_card_list = events.find_all(class_="sdc-site-team-lineup__item-event-icon--red-card")

                goals = second_function(goal_list)
                penalty_goals = second_function(penalty_goal_list)
                not_penalty_goals = second_function(not_penalty_goal_list)
                own_goals = second_function(own_goal_list)
                substitutions = second_function(substitution_list)
                assists = second_function(assist_list)
                yellow_cards = second_function(yellow_card_list)
                red_cards = second_function(red_card_list)

        except AttributeError:
            goals = []
            penalty_goals = []
            not_penalty_goals = []
            own_goals = []
            substitutions = []
            assists = []
            yellow_cards = []
            red_cards = []

        if name is "":
            name = surname
        else:
            name += " " + surname
        row_values = {
            "match_id": match_id,
            "number_player": number,
            "name_player": name,
            "substitution": substitutions,
            "goals_time": goals,
            "penalty_goals": penalty_goals,
            "not_penalty_goals": not_penalty_goals,
            "own_goals_time": own_goals,
            "assist_time": assists,
            "yellow_card_time": yellow_cards,
            "red_card_time": red_cards
        }
        if team_flag:
            players_statistics_home_team.append(row_values)
        else:
            players_statistics_away_team.append(row_values)


def second_function(value_list):
    find_values = []
    for value in value_list:
        minute = value.find_next(class_="sdc-site-team-lineup__event_time").text.strip()
        find_values.append(minute)
    return find_values


# Create a list that contains a pair of teams for the current match_id and detailed match statistics

match_officials = []

# Open the file that contains URLs of all matches
file_path = os.path.abspath("../urls/file_players_url.txt")
with open(file_path, "r") as file:
    for url in file:
        response = requests.get(url.strip(), timeout=10)
        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        team_blocks = soup.find_all('div', class_='sdc-site-team-lineup__header sdc-site-team-lineup__header--main')
        flag_team = True
        for team in team_blocks:
            main_players = team.find_next_sibling('dl').find_all('dd', class_='sdc-site-team-lineup__player-name')

            first_function(int(url[-7:]), main_players, flag_team)
            substitutes = team.find_next_sibling('h5', class_='sdc-site-team-lineup__header--subs')
            if substitutes:
                substitutes_players = substitutes.find_next_sibling('dl').find_all('dd', class_='sdc-site-team'
                                                                                                '-lineup__player-name')
                first_function(int(url[-7:]), substitutes_players, flag_team)
            flag_team = False

data_players_statistics_home_team = pd.DataFrame(players_statistics_home_team)
data_players_statistics_away_team = pd.DataFrame(players_statistics_away_team)

data_players_statistics_home_team = data_players_statistics_home_team.set_index('match_id')
data_players_statistics_home_team.to_csv('../data/players_statistics_home_team.csv')

data_players_statistics_away_team = data_players_statistics_away_team.set_index('match_id')
data_players_statistics_away_team.to_csv('../data/players_statistics_away_team.csv')
