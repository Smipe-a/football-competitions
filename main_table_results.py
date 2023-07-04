import pandas as pd
import requests
from bs4 import BeautifulSoup

# Send GET-request on web-site
url = "https://www.skysports.com/premier-league-table/2022"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

# Find main table on site
table = soup.find("table", class_="standing-table__table")

# Create a list to receive data from page
main_table_results = []

# Iterate through each row of the table
for row in table.find_all("tr"):
    # Retrieve the value of a cell
    cells = row.find_all("td")
    # Create a dictionary with the parsed data
    if len(cells) > 0:
        row_data = {
            "position": cells[0].text.strip(),
            "team": cells[1].find("a", class_="standing-table__cell--name-link").text.strip(),
            "played": cells[2].text.strip(),
            "won": cells[3].text.strip(),
            "drawn": cells[4].text.strip(),
            "lost": cells[5].text.strip(),
            "goals_for": cells[6].text.strip(),
            "goals_against": cells[7].text.strip(),
            "goals_difference": cells[8].text.strip(),
            "points": cells[9].text.strip()
        }
        main_table_results.append(row_data)

data_main_table_results = pd.DataFrame(main_table_results)
data_main_table_results = data_main_table_results.set_index('position')
data_main_table_results.to_csv('main_table_results.csv')
