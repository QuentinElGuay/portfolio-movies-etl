from collections.abc import Iterator
import logging
from typing import Any
from urllib.parse import urljoin
import requests

from movie_etl.config import Settings


logger = logging.getLogger(f'movie_etl.{__name__}')


MAX_PAGE_SIZE = 2000

AUTH_ENDPOINT = '/auth'
GENRES_ENDPOINT = '/art/v3/genres'
MOVIES_ENDPOINT = '/art/v3/movies'
MOVIE_RATINGS_ENDPOINT = '/art/v3/ratings'


class ApiClient:
    def __init__(self, session: requests.Session, settings: Settings):

        self.session = session
        self.settings = settings

    def get_auth(
        self, endpoint: str, username: str, password: str, timeout: int = 5
    ) -> str:
        """
        Returns a token from an basic authentication endpoint
        """
        response = self.session.post(
            urljoin(self.settings.api_base_url, endpoint),
            json={'username': username, 'password': password},
            timeout=timeout,
        )

        try:
            response.raise_for_status()
            logger.debug('Successul authentication to API.')

        except Exception as error:
            logger.error('Unable to authenticate to API: %s.', error)
            raise

        return response.json()['access_token']

    def get_endpoint(self, endpoint: str, timeout: int = 5) -> Iterator[dict[str, Any]]:
        """
        Returns the result of a GET call to an endpoint
        """
        url = urljoin(self.settings.api_base_url, endpoint)
        params = {'limit': MAX_PAGE_SIZE}

        while url:
            response = self.session.get(url, params=params, timeout=timeout)

            try:
                response.raise_for_status()

            except Exception as error:
                logger.error('Unable to retrieve endpoint %s: %s', url, error)
                raise

            payload = response.json()

            yield from payload['data']

            next_url = response.headers.get('Link', '').split(';')[0].strip('<>')
            if next_url:
                url = urljoin(self.settings.api_base_url, next_url)
                params = None
            else:
                url = None
