import psycopg2
from decouple import config


def create_schema(schema, connection):
    try:
        cursor = connection.cursor()
        # Check if a schema with this season already exists
        check_schema_query = """
                             SELECT nspname
                             FROM pg_namespace
                             WHERE nspname = %s
                             """
        cursor.execute(check_schema_query, (schema, ))

        if cursor.fetchone() is None:
            create_schema_query = f'CREATE SCHEMA {schema};'
            cursor.execute(create_schema_query)
            # Implementing the output of exceptions and messages to a log file
            connection.commit()
            print('Schema was created')
        else:
            # Implementing the output of exceptions and messages to a log file
            print(f'Schema {schema} is exist')

        cursor.close()
    except Exception as e:
        # Implementing the output of exceptions and messages to a log file
        print(e)


def create_table(schema, connection):
    # Normalization tables:
    # standings -> 2NF
    dict_query = {
        'standings': f"""
                     CREATE TABLE {schema}.standings (
                        team VARCHAR(35) PRIMARY KEY,
                        position INT UNIQUE,
                        played INT,
                        won INT,
                        drawn INT,
                        lost INT,
                        goals_for INT,
                        goals_against INT,
                        goals_difference INT,
                        points INT
                     );
                     """
    }

    try:
        cursor = connection.cursor()
        # Check if a table with this season already exists
        check_table_query = """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                            """
        cursor.execute(check_table_query, (schema, 'standings'))

        if cursor.fetchone() is None:
            create_table_query = (
                dict_query['standings']
            )
            cursor.execute(create_table_query)
            # Implementing the output of exceptions and messages to a log file
            connection.commit()
            print('Successful')
        else:
            # Implementing the output of exceptions and messages to a log file
            print('Table is exist')

        cursor.close()
        connection.close()
    except Exception as e:
        # Implementing the output of exceptions and messages to a log file
        print(e)


if __name__ == '__main__':
    database_params = {
        'dbname': 'english_premier_league',
        'user': config('PG_USER'),
        'password': config('PG_PASSWORD'),
        'host': config('PG_HOST'),
        'port': '5432'
    }
    connection_database = psycopg2.connect(**database_params)

    # Create a variable for the current game season
    with open('../resources/gameweek_dates.txt', 'r') as file:
        # Obtain the start date of the first round via
        # the API and transform the received year into the following format:
        # 2023-08-11 -> season23_24
        season = file.readline()[2:4]
        season = f'season{season}_{int(season) + 1}'

    create_schema(season, connection_database)
    create_table(season, connection_database)
