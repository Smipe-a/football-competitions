import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

# Open the web page using the Selenium library
driver = webdriver.Chrome()
url = 'https://www.skysports.com/premier-league-results/2022-23'
driver.get(url)

# Wait for the cookie acceptance popup to appear
wait = WebDriverWait(driver, 30)
frame = wait.until(ec.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR,
                                                              'iframe[src^="https://cdn.privacy-mgmt.com"]')))

# Wait for the 'Accept all' button to appear inside the frame and click on it
accept_button = wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR,
                                                       'button.message-component.message-button[title="Accept all"]')))
accept_button.click()
driver.switch_to.default_content()

# Since the initial page does not contain all the data we need,
# which is hidden behind the 'Show More' button,
# we use Selenium to click on it and obtain the HTML link for data parsing
load_more_button = wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, 'button.plus-more')))
time.sleep(30)
html = driver.page_source

soup = BeautifulSoup(html, "html.parser")
body = soup.find('div', class_='fixres__body')
matches = body.find_all('div', class_='fixres__item')

# Create a list to store the data about the total number of matches and their results
match_table_results = []
# List urls for parsing players statistics and match statistics
players_path_url = []
teams_path_url = []


months = {'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05',
          'June': '06', 'July': '07', 'August': '08', 'September': '09', 'October': '10',
          'November': '11', 'December': '12'}
number = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

for match in matches:
    match_id = match.find('a', class_='matches__item')['data-item-id']
    home_team = match.find(class_='matches__participant--side1').text.strip()
    away_team = match.find(class_='matches__participant--side2').text.strip()
    year = match.find_previous('h3', class_='fixres__header1').text.strip().split()[-1]
    date_without_year = match.find_previous('h4', class_='fixres__header2').text.strip().split(' ')[1:]

    date_with_year = year + "/"
    date_with_year += months[date_without_year[1]] + "/"
    if len(date_without_year[0]) == 3:
        date_with_year += '0' + date_without_year[0][0]
    else:
        date_with_year += date_without_year[0][0] + date_without_year[0][1]

    # Create links for further parsing of match statistics and team statistics into separate files
    path_teams = home_team.replace(' ', '-').lower() + '-vs-' + away_team.replace(' ', '-').lower()
    players_path_url.append("https://www.skysports.com/football/" + path_teams + "/teams/" + match_id)
    teams_path_url.append("https://www.skysports.com/football/" + path_teams + "/stats/" + match_id)

    row_data = {
        "match_id": match_id,
        "home_team": home_team,
        "away_team": away_team,
        "score_ht": match.find(class_='matches__teamscores-side').text.strip(),
        "score_at": match.find(class_='matches__teamscores-side').find_next_sibling().text.strip(),
        "date_start": date_with_year + " " + match.find(class_='matches__date').text.strip()
    }
    match_table_results.append(row_data)

data_match_table_results = pd.DataFrame(match_table_results)
data_match_table_results['date_start'] = pd.to_datetime(data_match_table_results['date_start'])
data_match_table_results = data_match_table_results.set_index('match_id')

data_match_table_results.to_csv('match_table_results.csv')

file_players_url = open('file_players_url.txt', 'w')
players_path_url = map(lambda x: x + '\n', players_path_url)
file_players_url.writelines(players_path_url)
file_players_url.close()

file_teams_url = open('file_teams_url.txt', 'w')
teams_path_url = map(lambda x: x + '\n', teams_path_url)
file_teams_url.writelines(teams_path_url)
file_teams_url.close()
