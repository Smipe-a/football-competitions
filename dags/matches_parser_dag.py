from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os

# Get the absolute path to the parent directory of the curresnt file
# and append this path to sys.path so that Python can find modules from this directory.
# Path: <your_abspath>/football-competitions/
PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG = 'resources'

sys.path.append(PROJECT_DIRECTORY)
from utils.link_mapper import format_string
from utils.constants import HASHMAP_LEAGUE_IDS
from scripts import matches, stadiums, match_details

# Current date from which the DAG should start executing
DATE_START_PARSE = datetime(2024, 7, 1, 18)

default_args = {
    'owner': 'Artyom Kruglov',
    'depends_on_past': False,
    'start_date': DATE_START_PARSE,
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG(
    dag_id="matches_parser",
    default_args=default_args,
    description='test',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    for league in HASHMAP_LEAGUE_IDS:
        formatted_league = format_string.format_string(league)

        matches_parse = PythonOperator(
            task_id=f'matches_{formatted_league}',
            python_callable=matches.main,
            op_args=[league],
            provide_context=True,
            dag=dag,
        )

        stadiums_task = PythonOperator(
            task_id=f'stadiums_{formatted_league}',
            python_callable=stadiums.main,
            op_args=[league],
            provide_context=True,
            dag=dag,
        )

        results_task = PythonOperator(
            task_id=f'match_details_{formatted_league}',
            python_callable=match_details.main,
            op_args=[league],
            provide_context=True,
            dag=dag,
        )

        matches_parse >> stadiums_task >> results_task
