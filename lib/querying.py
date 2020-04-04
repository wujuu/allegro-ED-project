import os
import itertools
import requests
import multiprocessing

from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

LIMIT_PER_QUERY = 100  # Max is 100 according to Allegro REST API
HOST = 'https://api.allegro.pl'


def get_items(phrase, category_id=None):
    """
    :param phrase:
       Search word. Example: "mydlo"
    :param category_id:
        Optional category_id from Allegro API. Default is None - search in all categories.
    :return:
        Raw query item data to be processed by Pandas.
    """
    cpus = multiprocessing.cpu_count()
    starting_offset = 0
    all_items = []
    token = _get_allegro_token()

    while True:
        items = _concurrent_item_query(phrase, starting_offset, token, category_id)
        all_items += items

        if len(items) < cpus * LIMIT_PER_QUERY:
            break

        starting_offset += cpus * LIMIT_PER_QUERY

    return all_items


def _get_allegro_token():
    token_url = os.getenv('TOKEN_URL')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    auth = HTTPBasicAuth(client_id, client_secret)
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=token_url, auth=auth)
    return token['access_token']


def _generic_query(url, token):
    return requests.get(url,
                        headers={
                            'Authorization': 'Bearer {0}'.format(token),
                            'Accept': 'application/vnd.allegro.public.v1+json'
                        }).json()


def _parse_item_search_url(phrase, offset, category_id=None):
    base_url = f'{HOST}/offers/listing'
    query_url = f"{base_url}?phrase={phrase}&offset={offset}&limit={LIMIT_PER_QUERY}"
    query_url += f"&{category_id}" if category_id else ""

    return query_url


def _item_query(phrase, offset, token, category_id=None):
    query_url = _parse_item_search_url(phrase, offset, category_id)
    query_result = _generic_query(query_url, token)

    if query_result.get('items') is None:
        return None

    return query_result['items']['regular'] + query_result['items']['promoted']


def _concurrent_item_query(phrase, starting_offset, token, category_id=None):
    cpus = multiprocessing.cpu_count()
    offsets = [starting_offset + i * LIMIT_PER_QUERY for i in range(cpus)]

    with ThreadPoolExecutor(max_workers=cpus) as executor:
        futures = [executor.submit(_item_query, phrase, offset, token, category_id) for offset in offsets]
        gathered_items = [future.result() for future in futures if future.result() is not None]
        return list(itertools.chain(*gathered_items))
