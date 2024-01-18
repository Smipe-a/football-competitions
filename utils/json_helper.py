from utils.logger import configure_logger
from typing import Optional, Any
import json


class JsonHelper:
    def __init__(self):
        self.path: Optional[str] = None
        self.value_json: Optional[dict] = None
        self.logger = configure_logger(__name__, 'json_helper')

    def read(self, path: str) -> Optional['JsonHelper']:
        """
        Read JSON data from a file.

        Args:
            path (str): The absolute path to the JSON file.

        Returns:
            Optional['JsonHelper']: An instance of JsonHelper if reading is successful, None otherwise.
        """
        try:
            with open(path, 'r') as file:
                self.path = path
                self.value_json = json.load(file)
            return self
        except FileNotFoundError:
            self.logger.error(f'File not found at path: {path}.')
        except json.JSONDecodeError:
            self.logger.error(f'Invalid JSON format in file at path: {path}.')

    def write(self) -> bool:
        """
        Write JSON data to the file.

        Returns:
            bool: True if writing is successful, False otherwise.
        """
        try:
            with open(self.path, 'w') as file:
                json.dump(self.value_json, file, indent=4, ensure_ascii=False)
            return True
        except FileNotFoundError:
            self.logger.error(f'File not found at path: {self.path}.')
        except json.JSONDecodeError:
            self.logger.error(f'Invalid JSON format in file at path: {self.path}.')

    def get(self, key: Optional[str] = None, subkey: Optional[str] = None) -> Optional[Any]:
        """
        Get the value from JSON data.

        Args:
            key (str): The key in the JSON data. The key can only take values of football competitions. The
                full list can be found in the documentation.
            subkey (str): The subkey in the JSON data.

        Returns:
            Any: The corresponding value or None if not found.
        """
        if key is None:
            return self.value_json
        elif key in self.value_json:
            if subkey is None:
                return self.value_json[key]
            elif subkey in self.value_json[key]:
                return self.value_json[key][subkey]
        self.logger.warning(f'The value with key {key} and subkey {subkey} was not found.')
        return None

    def append(self, key: str, subkey: str, value: Any = None) -> bool:
        """
        Append a value to the JSON data.

        Args:
            key (str): The key in the JSON data. The key can only take values of football competitions. The
                full list can be found in the documentation.
            subkey (str): The subkey in the JSON data.
            value (Any): The value to append.

        Returns:
            bool: True if appending is successful, False otherwise.
        """
        if key in self.value_json:
            self.value_json[key].update({subkey: value})
            return True
        self.logger.warning(f'Value was not added for key {key} and subkey {subkey}.')
        return False
