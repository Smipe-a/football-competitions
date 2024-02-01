from typing import List
import os

# The order of elements is important in the DATABASE_TABLE_NAME list
DATABASE_TABLE_NAME: List[str] = [
    'standings', 'team_clashes', 'result_clashes', 'info_clashes', 'stadiums', 'addresses',
    'lineups', 'home_match_statistics', 'away_match_statistics'
]
LEAGUES: List[str] = [
    'en_premier_league', 'la_liga', 'ligue_1', 'bundesliga',
    'serie_a', 'ru_premier_league', 'saudi_pro_league', 'kz_premier_league'
]

# We obtain the current directory and its parent directory.
# An absolute path is constructed based on the parent path
PROJECT_DIRECTORY: str = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG: str = 'resources'
LOG_CATALOG: str = 'logs'

METADATA_PARSER_FILE_LOG: str = 'metadata_parser.log'
CLASHES_FILE_LOG: str = 'clashes.log'
STATISTICS_FILE_LOG: str = 'statistics.log'
STANDINGS_FILE_LOG: str = 'standings.log'
DATABASE_INFO_FILE_LOG: str = 'database_info.log'

COMPETITIONS_JSON: str = 'competitions.json'
