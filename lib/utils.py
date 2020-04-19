import json
import os


def load_config():
    this_file_path = os.path.abspath(os.path.dirname(__file__))
    relative_config_path = os.path.join(this_file_path, "../config.json")

    with open(relative_config_path) as config_file:
        config = json.load(config_file)

    return config


def prepare_logger():
    import logging

    this_file_path = os.path.abspath(os.path.dirname(__file__))
    relative_log_path = os.path.join(this_file_path, '../logs')

    if not os.path.exists(relative_log_path):
        os.mkdir(relative_log_path)

    this_file_path = os.path.abspath(os.path.dirname(__file__))
    log_path = os.path.join(this_file_path, f'../logs/dev.log')
    logging.basicConfig(level=logging.INFO, filename=log_path, format='%(asctime)s :: %(levelname)s :: %(message)s')
