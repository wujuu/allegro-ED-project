import os
import pandas as pd
import logging
import time

from lib.querying import _get_subcategories, _get_items
from lib.processing import _construct_categories_df, _process_items, _get_relevant_categories, _glue_items
from lib.utils import load_config, prepare_logger

config = load_config()
prepare_logger()


def _get_category_id(category_name, parent_id=None):
    parent_subcategories = _construct_categories_df(_get_subcategories(parent_id))
    return parent_subcategories.at[category_name, 'id']


def _get_tree_ids(tree):
    ids = []

    main_category = tree[0]
    main_category_id = _get_category_id(main_category)
    ids.append(main_category_id)

    parent_id = main_category_id
    for category in tree[1:]:
        parent_id = _get_category_id(category, parent_id)
        ids.append(parent_id)

    return ids


def _save(items_df, phrase, save_mode):
    this_file_path = os.path.abspath(os.path.dirname(__file__))
    relative_db_path = os.path.join(this_file_path, f'../db')

    if not os.path.exists(relative_db_path):
        os.mkdir(relative_db_path)

    if save_mode == 'append':
        relative_csv_path = os.path.join(this_file_path, f'../db/{phrase}.csv')

        if not os.path.exists(relative_csv_path):
            open(relative_csv_path, mode='w+').close()
            items_df.to_csv(relative_csv_path)
        else:
            old_items_df = load(phrase)
            glued = _glue_items([old_items_df, items_df])
            glued.to_csv(relative_csv_path)
    else:  # save mode == 'new file'
        relative_csv_path = os.path.join(this_file_path, f'../db/{phrase}_{int(time.time())}.csv')
        items_df.to_csv(relative_csv_path)

    logging.info(f"Successefully saved items to file {relative_csv_path}")


def load(phrase):
    this_file_path = os.path.abspath(os.path.dirname(__file__))
    relative_csv_path = os.path.join(this_file_path, f'../db/{phrase}.csv')

    return pd.read_csv(relative_csv_path, index_col=0, dtype={'id': str})


def mine(phrase, save_mode='new_file'):
    item_per_category_threshold = config['mining']['item_per_category_threshold']
    root_items = _get_items(phrase)

    if root_items is None:
        logging.error(f"Could not fetch root items for phrase {phrase}")
        return

    root_items_df = _process_items(root_items)

    relevant_categories = _get_relevant_categories(root_items_df, item_per_category_threshold)
    every_category_items = [root_items_df]

    for category in relevant_categories:
        category_items = _get_items(phrase, category)

        if category_items is None:
            logging.error(f"Could not fetch items in category {category} for phrase {phrase}")
            continue

        category_items_df = _process_items(category_items)
        every_category_items.append(category_items_df)

    all_items_df = _glue_items(every_category_items)
    logging.info(f"Successfully fetched and parsed data for phrase {phrase}")

    _save(all_items_df, phrase, save_mode)

    return all_items_df


def mine_all():
    phrases = config['mining']['phrases']

    for phrase in phrases:
        mine(phrase)
