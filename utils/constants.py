from typing import List, Dict, Optional
import os

# The order of elements is important in the DATABASE_TABLE_NAME list
DATABASE_FIRST_TABLES: List[str] = [
    'teams', 'matches', 'match_lineups',
    'match_results', 'stadiums', 'match_details',
    'players', 'transfers'
]
DATABASE_SECOND_TABLES: List[str] = [
    'cards', 'outfield_players', 'goalkeepers', 'prices'
]
LEAGUES: List[str] = [
    'en_premier_league', 'la_liga', 'ligue_1', 'bundesliga',
    'serie_a', 'ru_premier_league', 'saudi_pro_league', 'kz_premier_league'
]
TRANSFERMARKT_SUFFIX_URL: List[List[str]] = [
    ['premier-league', 'GB1'],
    ['laliga', 'ES1'],
    ['ligue-1', 'FR1'],
    ['bundesliga', 'L1'],
    ['serie-a', 'IT1'],
    ['premier-liga', 'RU1'],
    ['saudi-professional-league', 'SA1'],
    ['premier-liga', 'KAS1']
]
FOTMOB_SUFFIX_URL: List[List[str]] = [
    ['47', 'premier-league'], ['87', 'laliga'], ['53', 'ligue-1'], ['54', 'bundesliga'],
    ['55', 'serie'], ['63', 'premier-league'], ['536', 'saudi-pro-league'], ['225', 'premier-league']
]
HASHMAP_LEAGUE_IDS: Dict[str, List[Optional[str]]] = {
    'Liga Profesional de Fútbol': ['112', 'liga-profesional',
                                   'copa-de-la-liga-profesional-de-futbol', 'CDLP'],
    'CONMEBOL Sudamericana': ['299', 'copa-sudamericana',
                              'copa-sudamericana', 'CS'],
    '3F Superliga': ['46', 'superligaen',
                     'superliga', 'DK1'],
    'cinch Premiership': ['64', 'premiership',
                          'scottish-premiership', 'SC1'],
    'Eliteserien': ['59', 'eliteserien',
                    'eliteserien', 'NO1'],
    'Google Pixel Frauen-Bundesliga': ['9676', 'frauen-bundesliga',
                                       None, None],
    "Scottish Women's League": [None, None,
                                None, None],
    'Nederland Vrouwen Liga': ['10289', 'eredivisie-vrouwen',
                               None, None],
    'LALIGA EA SPORTS': ['87', 'laliga',
                         'laliga', 'ES1'],
    'Ukrayina Liha': ['441', 'premier-league',
                      'premier-liga', 'UKR1'],
    'D1 Arkema': ['9677', 'division-1-feminine',
                  None, None],
    'Liga Hrvatska': ['252', 'hnl',
                      '1-hnl', 'KR1'],
    "National Women's Soccer League": ['9134', 'nwsl',
                                       None, None],
    'LALIGA HYPERMOTION': ['140', 'laliga2',
                           'laliga2', 'ES2'],
    '1A Pro League': ['40', 'first-division',
                      'jupiler-pro-league', 'BE1'],
    'Liga Cyprus': ['136', '1-division',
                    'protathlima-cyta', 'ZYP1'],
    'Allsvenskan': ['67', 'allsvenskan',
                    'allsvenskan', 'SE1'],
    'Barclays Women’s Super League': ['9227', 'wsl',
                                      None, None],
    'Bundesliga': ['54', 'bundesliga',
                   'bundesliga', 'L1'],
    'Liga Portugal Feminino': [None, None,
                               None, None],
    'SUPERLIGA': ['189', 'liga-i',
                  'liga-1', 'RO1'],
    'Österreichische Fußball-Bundesliga': ['38', 'bundesliga',
                                           'bundesliga', 'A1'],
    'Sverige Liga': ['9089', 'damallsvenskan',
                     None, None],
    'Ligue 1 Uber Eats': ['53', 'ligue-1',
                          'ligue-1', 'FR1'],
    'Ligue 2 BKT': ['110', 'ligue-2',
                    'ligue-2', 'FR2'],
    'Calcio A Femminile': ['10178', 'serie-femminile',
                           None, None],
    'Credit Suisse Super League': ['69', 'super-league',
                                   'super-league', 'C1'],
    'Trendyol Süper Lig': ['71', 'super-lig',
                           'super-lig', 'TR1'],
    'Serie BKT': ['86', 'serie-b',
                  'serie-b', 'IT2'],
    'EFL Championship': ['48', 'championship',
                         'championship', 'GB2'],
    'PKO Bank Polski Ekstraklasa': ['196', 'ekstraklasa',
                                    'pko-ekstraklasa', 'PL1'],
    'Premier League': ['47', 'premier-league',
                       'premier-league', 'GB1'],
    'Chinese Football Association Super League': ['120', 'super-league',
                                                  'chinese-super-league', 'CSL'],
    'Hellas Liga': ['135', 'super-league-1',
                    'super-league-1', 'GR1'],
    "SSE Airtricity Men's Premier Division": ['126', 'premier-division',
                                              'premier-league', 'IR1'],
    'Major League Soccer': ['130', 'mls',
                            'major-league-soccer', 'MLS1'],
    'Česká Liga': ['122', '1-liga',
                   'fortuna-liga', 'TS1'],
    'Bundesliga 2': ['146', '2-bundesliga',
                     '2-bundesliga', 'L2'],
    'CONMEBOL Libertadores': ['45', 'copa-libertadores',
                              'copa-libertadores', 'CLI'],
    'Magyar Liga': ['212', 'nb-i',
                    'nemzeti-bajnoksag', 'UNG1'],
    'K League 1': ['9080', 'k-league-1',
                   'k-league-1', 'RSK1'],
    'ROSHN Saudi League': ['536', 'saudi-pro-league',
                           'saudi-professional-league', 'SA1'],
    'Isuzu UTE A League': ['113', 'league',
                           'a-league', 'AUS1'],
    'EFL League Two': ['109', 'league-two',
                       'league-two', 'GB4'],
    'Ceska Liga Žen': [None, None,
                       None, None],
    'Eredivisie': ['57', 'eredivisie',
                   'eredivisie', 'NL1'],
    'Schweizer Damen Liga': [None, None,
                             None, None],
    'United Emirates League': ['538', 'pro-league',
                               'uae-arabian-gulf-league', 'UAE1'],
    'Hero Indian Super League': ['9478', 'super-league',
                                 'indian-super-league', 'IND1'],
    'Finnliiga': ['51', 'veikkausliiga',
                  'veikkausliiga', 'FI1'],
    'Liga Portugal': ['61', 'liga-portugal',
                      'liga-nos', 'PO1'],
    'Serie A TIM': ['55', 'serie',
                    'serie-a', 'IT1'],
    '3. Liga': ['208', '3-liga',
                '3-liga', 'L3'],
    'EFL League One': ['108', 'league-one',
                       'league-one', 'GB3'],
    'Liga F': ['9907', 'liga-f',
               None, None]
}

# We obtain the current directory and its parent directory.
# An absolute path is constructed based on the parent path
PROJECT_DIRECTORY: str = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
RESOURCE_CATALOG: str = 'resources'
LOG_CATALOG: str = 'logs'

EAFC_CARDS_FILE_LOG: str = 'eafc_cards.log'
EAFC_PARAMETERS_FILE_LOG: str = 'eafc_parameters.log'
EAFC_PRICES_FILE_LOG: str = 'eafc_prices.log'

METADATA_PARSER_FILE_LOG: str = 'metadata_parser.log'
TRANSFERMARKT_PLAYERS_LOGS: str = 'players.log'
LINK_MAPPER_FILE_LOG: str = 'link_mapper.log'
MATCHES_FILE_LOG: str = 'matches.log'
MATCH_RESULTS_FILE_LOG: str = 'match_results.log'
CLASHES_FILE_LOG: str = 'clashes.log'
STATISTICS_FILE_LOG: str = 'statistics.log'

STADIUMS_FILE_LOG: str = 'stadiums.log'

DATABASE_INFO_FILE_LOG: str = 'database_info.log'

COMPETITIONS_JSON: str = 'competitions.json'
