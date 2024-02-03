from typing import List
import os

# The order of elements is important in the DATABASE_TABLE_NAME list
DATABASE_TABLES: List[str] = [
    'standings', 'clashes', 'result_clashes', 'stadiums', 'info_clashes', 'addresses', 'lineups'
]
LEAGUES: List[str] = [
    'en_premier_league', 'la_liga', 'ligue_1', 'bundesliga',
    'serie_a', 'ru_premier_league', 'saudi_pro_league', 'kz_premier_league'
]
TRANSFERMARKT_SUFFIX_URL: List[List[str]] = [
    ['premier-league', 'GB1'], ['laliga', 'ES1'], ['ligue-1', 'FR1'], ['bundesliga', 'L1'],
    ['serie-a', 'IT1'], ['premier-liga', 'RU1'], ['saudi-professional-league', 'SA1'], ['premier-liga', 'KAS1']
]
FOTMOB_SUFFIX_URL: List[List[str]] = [
    ['47', 'premier-league'], ['87', 'laliga'], ['53', 'ligue-1'], ['54', 'bundesliga'],
    ['55', 'serie'], ['63', 'premier-league'], ['536', 'saudi-pro-league'], ['225', 'premier-league']
]

# We obtain the current directory and its parent directory.
# An absolute path is constructed based on the parent path
PROJECT_DIRECTORY: str = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG: str = 'resources'
LOG_CATALOG: str = 'logs'

METADATA_PARSER_FILE_LOG: str = 'metadata_parser.log'
LINK_MAPPER_FILE_LOG: str = 'link_mapper.log'
CLASHES_FILE_LOG: str = 'clashes.log'
STATISTICS_FILE_LOG: str = 'statistics.log'
STANDINGS_FILE_LOG: str = 'standings.log'

DATABASE_INFO_FILE_LOG: str = 'database_info.log'

COMPETITIONS_JSON: str = 'competitions.json'
