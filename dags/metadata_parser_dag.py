from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os

# Get the absolute path to the parent directory of the current file
# and append this path to sys.path so that Python can find modules from this directory.
# Path: <your_abspath>/football-competitions/
PROJECT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(PROJECT_DIRECTORY)
from scripts import metadata_parser

# The provided date is an approximate start date for the competitions listed in COMPETITIONS_TITLE
# that have not yet started, but new dates are already available on the championat.com website
DATE_START_PARSE = datetime(2023, 7, 29)
COMPETITIONS_TITLE = ['premier_league', 'la_liga', 'ligue_1', 'bundesliga']

default_args = {
    'owner': 'Artyom Kruglov',
    'depends_on_past': False,
    'start_date': DATE_START_PARSE,
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG(
    dag_id='metadata_parser',
    default_args=default_args,
    description='Parse metadata and dates for football competitions from championat.com',
    schedule=timedelta(days=365),
    catchup=False,
) as dag:
    operator = PythonOperator(
        task_id='metadata_parser',
        python_callable=metadata_parser.main,
        op_args=[COMPETITIONS_TITLE],
        dag=dag,
    )
