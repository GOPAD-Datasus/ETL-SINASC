import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd


def load_data (url: str, params: dict) -> list:

    return [f'{url}DN{x}.parquet.gzip'
            for x in range(params['starting_year'],
                           params['ending_year'] + 1)]


def insert_data (df: pd.DataFrame):
    load_dotenv()

    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    db = os.getenv('db')

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    with engine.connect() as conn:
        with conn.begin():
            df.to_sql('sinasc',
                      con=conn,
                      if_exists="append")
