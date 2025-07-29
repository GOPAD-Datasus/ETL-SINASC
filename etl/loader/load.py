from etl.loader.utils import *


def load(params: dict):
    url = 'data/processed/'

    for item in load_data(url, params):
        insert_data(pd.read_parquet(item))