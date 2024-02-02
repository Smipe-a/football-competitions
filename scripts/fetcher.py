from typing import Optional
import requests


class Fetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_html(self, url: str) -> Optional[bytes]:
        """
        Fetch HTML content from the specified URL.

        Args:
            url (str): The URL from which to fetch the HTML content.

        Returns:
            Optional[bytes]: The fetched HTML content as bytes or None if an error occurs.
        """
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.content
        else:
            error_message = f'Error {response.status_code} occurred while fetching {url}.'
            raise requests.RequestException(error_message)
