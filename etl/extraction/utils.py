import os

import yaml


def get_urls () -> dict:
    """
    Returns the items from 'files' key from input.yaml

    returns:
        dict: Year (int): link (str) for all desirable files
    """
    with open ('input.yaml') as f:
        return yaml.safe_load(f)['files']


def make_data_folder () -> None:
    """
    Creates the folder structure to hold raw and
    processed files. The structure is defined below:

    + data: root folder
      - raw: raw data extracted from source
      - processed: processed data
    """
    base_dir = os.path.dirname(os.path.abspath(__name__))

    data_folder      = os.path.join(base_dir, 'data')
    raw_folder       = os.path.join(data_folder, 'raw')
    processed_folder = os.path.join(data_folder, 'processed')

    os.makedirs(raw_folder, exist_ok=True)
    os.makedirs(processed_folder, exist_ok=True)


