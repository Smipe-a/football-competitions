import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Create a list that contains a pair of teams for the current match_id and detailed match statistics
match_statistics = []

# Open the file that contains URLs of all matches
file_path = os.path.abspath("../urls/file_teams_url.txt")
with open(file_path, "r") as file:
    for url in file:
        response = requests.get(url.strip(), timeout=10)
        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        head = soup.find(class_="sdc-site-match-header__detail")

        # We check 'stadium' for the absence of a block in the HTML
        try:
            stadium = head.find(class_="sdc-site-match-header__detail-venue--with-seperator").text.strip()
        except AttributeError:
            stadium = head.find(class_="sdc-site-match-header__detail-venue").text.strip()

        # We check 'attendance' for the absence of a block in the HTML
        try:
            attendance_element = head.find('span', class_='sdc-site-match-header__detail-attendance')
            attendance = re.findall(r'\d+', attendance_element.text.split(':')[1].strip().replace(',', ''))
        except AttributeError:
            attendance = None
        if attendance is not None:
            attendance = int(''.join(attendance))

        # We set initial values that are the same for both teams
        # match_id, stadium, attendance, is_home_team
        if stadium[-1] == '.':
            stadium = stadium[:-1]
        if stadium == "St James' Park, Newcastle":
            stadium = "St. James' Park"
        home_team_statistics = [int(url[-7:]), stadium, attendance, True]
        away_team_statistics = [int(url[-7:]), stadium, attendance, False]

        body = soup.find(class_="sdc-site-match-header__content")
        home_team_statistics.append(body.find(class_="sdc-site-match-header__team-name").text.strip())
        away_team_statistics.append(body.find(class_="sdc-site-match-header__team-name--away").text.strip())

        # Finding the block in the HTML with the statistics and extracting it
        table_statistics = soup.find_all(class_="sdc-site-match-stats__stats")
        for block in table_statistics:
            block_info_home = block.find(class_="sdc-site-match-stats__stats-home")
            block_info_away = block.find(class_="sdc-site-match-stats__stats-away")
            home_team_statistics.append(block_info_home.find(class_="sdc-site-match-stats__val").text.strip())
            away_team_statistics.append(block_info_away.find(class_="sdc-site-match-stats__val").text.strip())

        for i in range(2):
            match_data = {
                'match_id': home_team_statistics[0],
                'stadium': home_team_statistics[1],
                'attendance': home_team_statistics[2],
                'is_home_team': home_team_statistics[3],
                'team': home_team_statistics[4],
                'possessions': home_team_statistics[5],
                'total_shots': home_team_statistics[6],
                'on_target': home_team_statistics[7],
                'off_target': home_team_statistics[8],
                'blocked': home_team_statistics[9],
                'passing': home_team_statistics[10],
                'clear_cut_chance': home_team_statistics[11],
                'corners': home_team_statistics[12],
                'offsides': home_team_statistics[13],
                'tackles': home_team_statistics[14],
                'aerial_duels': home_team_statistics[15],
                'saves': home_team_statistics[16],
                'fouls_committed': home_team_statistics[17],
                'fouls_won': home_team_statistics[18],
                'yellow_cards': home_team_statistics[19],
                'red_cards': home_team_statistics[20]
            }
            home_team_statistics = away_team_statistics
            match_statistics.append(match_data)

    match_statistics = pd.DataFrame(match_statistics)
    match_statistics = match_statistics.set_index('match_id')
    match_statistics['attendance'].fillna(match_statistics['attendance'].median(), inplace=True)
    match_statistics['attendance'] = match_statistics['attendance'].astype(int)
    match_statistics.to_csv('../data/match_statistics.csv')
