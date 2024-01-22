from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os

# Get the absolute path to the parent directory of the current file
# and append this path to sys.path so that Python can find modules from this directory.
# Path: <your_abspath>/football-competitions/
PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG = 'resources'

sys.path.append(PROJECT_DIRECTORY)
from utils.json_helper import JsonHelper
from scripts import standings, match_results

# Current date from which the DAG should start executing
DATE_START_PARSE = datetime(2024, 1, 22, 8)
METADATA_FILE_NAME = 'tournaments_metadata.json'
COMPETITIONS_TITLE = ['premier_league', 'la_liga', 'ligue_1', 'bundesliga']

default_args = {
    'owner': 'Artyom Kruglov',
    'depends_on_past': False,
    'start_date': DATE_START_PARSE,
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG(
    dag_id="competition_parser",
    default_args=default_args,
    description='DAG for running standings scripts based on competition start dates',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    for competition in COMPETITIONS_TITLE:
        metadata_json = JsonHelper()
        metadata_json.read(os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, METADATA_FILE_NAME))
        gameweeks = metadata_json.get(competition, 'start_date')

        current_date = datetime.today().strftime('%Y-%m-%d')

        if current_date in gameweeks:
            task_id = f'_{current_date}_{competition}'

            standings_parse = PythonOperator(
                task_id=f'standings_{task_id}',
                python_callable=standings.main,
                op_args=[competition],
                provide_context=True,
                dag=dag,
            )

            match_results_parse = PythonOperator(
                task_id=f'match_results_{task_id}',
                python_callable=match_results.main,
                op_args=[competition, current_date],
                provide_context=True,
                dag=dag,
            )

            standings_parse >> match_results_parse
