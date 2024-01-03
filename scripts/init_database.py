from db_connector import connect_to_database


def create_table(competition, connection):
    # The 'points' attribute is retained,
    # as the team may face a penalty resulting in a deduction of points
    init_standings = {
        'standings': f"""
                        CREATE TABLE {competition}.standings (
                            team VARCHAR(35),
                            season VARCHAR(5),
                            position INT NOT NULL,
                            won INT NOT NULL,
                            drawn INT NOT NULL,
                            lost INT NOT NULL,
                            goals_for INT NOT NULL,
                            goals_against INT NOT NULL,
                            points INT NOT NULL,
                            PRIMARY KEY (team, season)
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
        cursor.execute(check_table_query, (competition, 'standings'))

        if cursor.fetchone() is None:
            create_table_query = (
                init_standings['standings']
            )
            cursor.execute(create_table_query)
            # Implementing the output of exceptions and messages to a log file
            connection.commit()
            print('Successful')
        else:
            # Implementing the output of exceptions and messages to a log file
            print('Table is exist')

        cursor.close()
    except Exception as e:
        # Implementing the output of exceptions and messages to a log file
        print(e)
    finally:
        connection.close()


if __name__ == '__main__':
    create_table('premier_league', connect_to_database())
