import pandas as pd

from etl.loader.utils import create_db_engine, insert_data
from utils import load_data


def load():
    url = 'data/processed/'
    engine = create_db_engine()
    file_list = load_data(url, endswith='.parquet')

    with engine.connect() as conn:
        for item in file_list:
            try:
                df = pd.read_parquet(item)
                with conn.begin():
                    insert_data(conn, df, 'sinasc')
            except Exception as e:
                raise Warning(f'Failed to load {item}: {e}')
