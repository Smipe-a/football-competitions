from decouple import config
import psycopg2


def connect_to_database():
    try:
        return psycopg2.connect(database='english_premier_league',
                                user=config('PG_USER'),
                                password=config('PG_PASSWORD'),
                                host=config('PG_HOST'),
                                port='5432')
    except Exception as e:
        # Implementing the output of exceptions and messages to a log file
        print(e)
        return None
