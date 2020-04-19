import itertools
import requests
import multiprocessing
import os
import time
import logging

from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from lib.utils import load_config, prepare_logger

load_dotenv()
config = load_config()
prepare_logger()


def _get_items(phrase, category_id=None):
    item_limit_per_query = config['querying']['item_limit_per_query']
    token = _get_allegro_token()

    if token is None:
        return None

    cpus = multiprocessing.cpu_count()
    starting_offset = 0
    all_items = []

    print(f'Searching in {category_id if category_id else "ROOT"} category...')

    while True:
        items = _concurrent_item_query(phrase, starting_offset, token, category_id)
        all_items += items

        if len(items) < cpus * item_limit_per_query:
            break

        starting_offset += cpus * item_limit_per_query

    print(f'Found {len(all_items)} items...')

    return all_items


def _get_subcategories(category_id):
    host = config['querying']['host']
    token = _get_allegro_token()
    base_url = f'{host}/sale/categories'
    base_url += f'?parent.id={category_id}' if category_id else ''
    query_result = _generic_query(base_url, token)['categories']
    return query_result


def _get_category_name(category_id):
    host = config['querying']['host']
    token = _get_allegro_token()
    base_url = f'{host}/sale/categories/{category_id}'
    query_result = _generic_query(base_url, token)
    return query_result['name']


def _get_allegro_token(trials=3):
    token_url = config['querying']['token_url']
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    for _ in range(trials):
        try:
            auth = HTTPBasicAuth(client_id, client_secret)
            client = BackendApplicationClient(client_id=client_id)
            oauth = OAuth2Session(client=client)
            token = oauth.fetch_token(token_url=token_url, auth=auth)
            token_value = token['access_token']
        except:
            token_value = None

        if token_value is not None:
            break

        time.sleep(5)

    if token_value is None:
        logging.error("Could not fetch token...")

    return token_value


def _generic_query(url, token, trials=3):
    for _ in range(trials):
        try:
            response = requests.get(url,
                                    headers={
                                        'Authorization': 'Bearer {0}'.format(token),
                                        'Accept': 'application/vnd.allegro.public.v1+json'
                                    }).json()
        except:
            response = None

        if response is not None:
            break

        time.sleep(5)

    if response is None:
        logging.warning(f"Could not fetch query {url}...")

    return response


def _parse_item_search_url(phrase, offset, category_id=None):
    item_limit_per_query = config['querying']['item_limit_per_query']
    host = config['querying']['host']

    base_url = f'{host}/offers/listing'
    query_url = f"{base_url}?phrase={phrase}&offset={offset}&limit={item_limit_per_query}"
    query_url += f"&category.id={category_id}" if category_id else ""

    return query_url


def _item_query(phrase, offset, token, category_id=None):
    query_url = _parse_item_search_url(phrase, offset, category_id)
    query_result = _generic_query(query_url, token)

    if query_result is None:
        return None

    if query_result.get('items') is None:
        return None

    return query_result['items']['regular'] + query_result['items']['promoted']


def _concurrent_item_query(phrase, starting_offset, token, category_id=None):
    item_limit_per_query = config['querying']['item_limit_per_query']
    cpus = multiprocessing.cpu_count()
    offsets = [starting_offset + i * item_limit_per_query for i in range(cpus)]

    with ThreadPoolExecutor(max_workers=cpus) as executor:
        futures = [executor.submit(_item_query, phrase, offset, token, category_id) for offset in offsets]
        gathered_items = [future.result() for future in futures if future.result() is not None]
        return list(itertools.chain(*gathered_items))
