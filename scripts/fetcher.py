from typing import Optional, Any
import requests


class Fetcher:
    """
    A simple data fetching utility.

    This class provides methods to fetch data from a specified URL, supporting both HTML and JSON content types.
    """
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_data(self, url: str, content_type: str = 'html') -> Optional[Any]:
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            if content_type == 'json':
                return response.json()
            elif content_type == 'html':
                return response.content
            else:
                raise TypeError(f'Unsupported content type: {content_type}')

        except requests.RequestException as e:
            error_message = f'Error occurred while fetching "{url}": {str(e).strip()}.'
            raise requests.RequestException(error_message)
