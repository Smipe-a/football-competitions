from psycopg2 import connect
from decouple import config
import logging


def connect_to_database():
    try:
        return connect(database='football_competitions',
                       user=config('PG_USER'),
                       password=config('PG_PASSWORD'),
                       host=config('PG_HOST'),
                       port='5432')
    except Exception as e:
        # Implementing the output of exceptions and messages to a log file
        print(e)
        return None
