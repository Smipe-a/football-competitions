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
RESOURCE_FILE_NAME = 'tournaments_metadata.json'

sys.path.append(PROJECT_DIRECTORY)
from utils.json_helper import JsonHelper
from scripts import standings

# The provided date is an approximate start date for the competitions listed in COMPETITIONS_TITLE
# that have not yet started, but new dates are already available on the championat.com website
DATE_START_PARSE = datetime(2023, 1, 19)
COMPETITIONS_TITLE = ['premier_league', 'la_liga', 'ligue_1', 'bundesliga']

default_args = {
    'owner': 'Artyom Kruglov',
    'depends_on_past': False,
    'start_date': DATE_START_PARSE,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="competition_parser",
    default_args=default_args,
    description='DAG for running standings scripts based on competition start dates',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    for competition in COMPETITIONS_TITLE:
        json_object = JsonHelper()
        gameweeks = json_object.read(
            os.path.join(PROJECT_DIRECTORY, RESOURCE_CATALOG, RESOURCE_FILE_NAME)).get(competition, 'start_date')

        current_date = datetime.today().strftime('%Y-%m-%d')

        if current_date in gameweeks:
            task_id = f'competition_parser_{current_date}_{competition}'
            run_standings_script = PythonOperator(
                task_id=task_id,
                python_callable=standings.main,
                op_args=[competition],
                provide_context=True,
                dag=dag,
            )
