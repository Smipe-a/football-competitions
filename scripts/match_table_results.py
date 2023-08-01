import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


# Function to convert month name from string to numeric
def get_month_number(month_name: str) -> str:
    date = datetime.strptime(month_name, '%B')
    return date.strftime('%m')


# Formatting the output of date data
def parse_date(date: list, year: str) -> str:
    day = date[0][:-2]
    month = get_month_number(date[1])
    return f'{year}/{month}/{day}'


def accept_cookie_and_click_buttons(driver_chrome):
    # Wait for the cookie acceptance popup to appear
    wait = WebDriverWait(driver_chrome, 10)
    wait.until(ec.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR,
                                                          'iframe[src^="https://cdn.privacy-mgmt.com"]')))
    # Wait for the 'Accept all' button to appear inside the frame and click on it
    time.sleep(1)
    wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR,
                                           'button.message-component.message-button[title="Accept all"]'))).click()
    # Since the initial page does not contain all the data we need,
    # which is hidden behind the 'Show More' button,
    # we use Selenium to click on it and obtain the HTML link for data parsing
    wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, 'button.plus-more'))).click()


def scrape_data() -> list:
    # Open the web page using the Selenium library
    driver = webdriver.Chrome()
    driver.get('https://www.skysports.com/premier-league-results/2022-23')
    accept_cookie_and_click_buttons(driver)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    body = soup.find('div', class_='fixres__body')
    matches = body.find_all('div', class_='fixres__item')

    # Create a list to store the data about the total number of matches and their results
    match_table_results = []
    # List urls for parsing players statistics and match statistics
    players_path_url = []
    teams_path_url = []

    for match in matches:
        match_id = match.find('a', class_='matches__item')['data-item-id']
        home_team = match.find(class_='matches__participant--side1').text.strip()
        away_team = match.find(class_='matches__participant--side2').text.strip()

        date_with_year = parse_date(
            match.find_previous('h4', class_='fixres__header2').text.strip().split(' ')[1:],
            match.find_previous('h3', class_='fixres__header1').text.strip().split()[-1]
        )

        match_data = {
            'match_id': match_id,
            'home_team': home_team,
            'away_team': away_team,
            'score_ht': match.find(class_='matches__teamscores-side').text.strip(),
            'score_at': match.find(class_='matches__teamscores-side').find_next_sibling().text.strip(),
            'date_start': date_with_year + ' ' + match.find(class_='matches__date').text.strip()
        }
        match_table_results.append(match_data)

        # Create links for further parsing of match statistics and team statistics into separate files
        path_teams = f"{home_team.replace(' ', '-').lower()}-vs-{away_team.replace(' ', '-').lower()}"
        players_path_url.append(f'https://www.skysports.com/football/{path_teams}/teams/{match_id}')
        teams_path_url.append(f'https://www.skysports.com/football/{path_teams}/stats/{match_id}')

        with open('../urls/2022-23/file_players_url.txt', 'w') as file_players_url:
            file_players_url.writelines([url + '\n' for url in players_path_url])

        with open('../urls/2022-23/file_teams_url.txt', 'w') as file_teams_url:
            file_teams_url.writelines([url + '\n' for url in teams_path_url])
    return match_table_results


if __name__ == '__main__':
    data_match_table_results = pd.DataFrame(scrape_data()).set_index('match_id')
    data_match_table_results['date_start'] = pd.to_datetime(data_match_table_results['date_start'])
    data_match_table_results.to_csv('../data/2022-23/match_table_results.csv')
