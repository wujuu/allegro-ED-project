import os
import itertools
import requests
import pandas as pd

from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

CPUs = 1
LIMIT_PER_QUERY = 100  # Max is 100 according to Allegro REST API
HOST = 'https://api.allegro.pl'

def get_allegro_token():
    token_url = os.getenv('TOKEN_URL')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    auth = HTTPBasicAuth(client_id, client_secret)
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=token_url, auth=auth)
    return token['access_token']


def generic_query(url, token):
    return requests.get(url,
                        headers={
                            'Authorization': 'Bearer {0}'.format(token),
                            'Accept': 'application/vnd.allegro.public.v1+json'
                        }).json()


def parse_item_search_url(phrase, offset, category_id=None):
    base_url = f'{HOST}/offers/listing'
    query_url = f"{base_url}?phrase={phrase}&offset={offset}&limit={LIMIT_PER_QUERY}"
    query_url += f"&{category_id}" if category_id else ""

    return query_url


def item_query(phrase, offset, token, category_id=None):
    query_url = parse_item_search_url(phrase, offset, category_id)
    query_result = generic_query(query_url, token)

    if query_result.get('items') is None:
        return None

    return query_result['items']['regular'] + query_result['items']['promoted']


def concurrent_item_query(phrase, starting_offset, token):
    offsets = [starting_offset + i * LIMIT_PER_QUERY for i in range(CPUs)]

    with ThreadPoolExecutor(max_workers=CPUs) as executor:
        futures = [executor.submit(item_query, phrase, offset, token) for offset in offsets]
        gathered_items = [future.result() for future in futures if future.result() is not None]
        return list(itertools.chain(*gathered_items))


def get_all_items(query, token):
    starting_offset = 0
    all_items = []

    while True:
        items = concurrent_item_query(query, starting_offset, token)
        all_items += items

        if len(items) < CPUs * LIMIT_PER_QUERY:
            break

        starting_offset += CPUs * LIMIT_PER_QUERY

    return all_items


def filter_row(row):
    return pd.Series({
        'id': row['id'],
        'name': row['name'],
        'delivery_cost': float(row['delivery']['lowestPrice']['amount']),
        'cost': float(row['sellingMode']['price']['amount']),
        'stock': int(row['stock']['available']),
        'category_id': int(row['category']['id'])
    })


def construct_df(items):
    return pd.DataFrame(items).apply(filter_row, axis=1)


def chunkify(items):
    items_per_CPU = len(items) // CPUs
    chunks = [items[x:x + items_per_CPU] for x in range(0, len(items), items_per_CPU)]

    if len(chunks) > CPUs:
        i = 0
        for item in chunks[-1]:
            chunks[i].append(item)
            i = (i + 1) % CPUs

    return chunks[:CPUs]


def filter_all_items(items):
    chunks = chunkify(items)

    with ThreadPoolExecutor(max_workers=CPUs) as executor:
        futures = [executor.submit(construct_df, chunk) for chunk in chunks]
        gathered_dfs = [future.result() for future in futures]

        return pd.concat(gathered_dfs, ignore_index=True)


def main():
    token = get_allegro_token()
    items = get_all_items('zegarek', token)
    df = filter_all_items(items)

    print(df)


if __name__ == '__main__':
    main()
