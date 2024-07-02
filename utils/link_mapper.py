from utils.constants import LINK_MAPPER_FILE_LOG
from utils.logger import configure_logger

# Define constants for logging and directory structure
LOGGER = configure_logger(__name__, LINK_MAPPER_FILE_LOG)


def format_string(team_name: str) -> str:
    """
    Formats the team name string according to the specified template.
    """
    # Handle special character replacements
    special_replacements = {'ú': 'u', '-': '_', "'": '', '’': '', 'Ö': 'o', 'ß': 'ss', 'ü': 'u', 'Č': 'c', 'á': 'a', 'Ž': 'z', '.': ''}
    first_letter = {'1': 'first_', '3': 'third_'}

    formatted_string = ''
    for letter in team_name:
        if formatted_string == '' and letter in first_letter:
            formatted_string += first_letter[letter]
        elif letter in special_replacements:
            formatted_string += special_replacements[letter]
        else:
            formatted_string += letter

    # Convert to lowercase and replace spaces with hyphens
    formatted_string = formatted_string.lower().replace(' ', '_')

    return formatted_string
