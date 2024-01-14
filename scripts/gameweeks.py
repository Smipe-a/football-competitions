from utils.logger import configure_logger
from datetime import datetime, timedelta
import requests
import json
import os

RESOURCE_DIRECTORY = 'resources'
RESOURCE_FILE_NAME = 'competition_config.json'


def dates_competition_parse(competition_title: str) -> None:
    # Configure logger for the current module
    logger = configure_logger(__name__, competition_title)

    url_api = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    try:
        # Make the request to the API
        response = requests.get(url_api)
        response.raise_for_status()
        premier_league_data = response.json()

        # We obtain the current directory and its parent directory.
        # An absolute path is constructed based on the parent path
        project_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        resources_directory = os.path.join(project_directory, RESOURCE_DIRECTORY)
        os.makedirs(resources_directory, exist_ok=True)

        # Extract dates from the API response
        # The documentation explains why the 1st date is not being taken
        dates_competition = [event['deadline_time'][:10] for event in premier_league_data['events'][1:]]

        # Calculate and add the last start date
        # Last start date = last date in date_competition + 7 days
        last_start_date = datetime.strptime(premier_league_data['events'][-1]['deadline_time'][:10], '%Y-%m-%d')
        new_last_start_date = last_start_date + timedelta(days=7)
        dates_competition.append(new_last_start_date.strftime('%Y-%m-%d'))

        # Create a dictionary with competition dates and save to a JSON file
        dates_competition = {competition_title: {'date_start': dates_competition}}
        # Path file equal <your_path>/football-competitions/resources/football_competitions.json
        with open(os.path.join(resources_directory, RESOURCE_FILE_NAME), 'w') as file:
            json.dump(dates_competition, file, indent=2)

        logger.info(f'Successfully obtained {len(dates_competition[competition_title]["date_start"])} '
                    f'start dates of {competition_title} competition game weeks.')

    except requests.exceptions.RequestException as req_err:
        logger.error(f'Error fetching data for {competition_title}: {req_err}')
        raise
    except IsADirectoryError as dir_err:
        logger.error(f'Error writing to file: {dir_err}')
        raise
