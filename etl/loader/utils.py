import os

import pandas as pd
import sqlalchemy.engine
from dotenv import load_dotenv
from sqlalchemy import create_engine


def create_db_engine () -> sqlalchemy.engine.Engine:
    load_dotenv()

    required_env = ['user', 'password', 'host', 'port', 'db']
    if any(os.getenv(var) is None for var in required_env):
        raise EnvironmentError("Incomplete .env file")

    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    db = os.getenv('db')

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    return engine


def insert_data (conn: sqlalchemy.engine.Connection,
                 df: pd.DataFrame,
                 table_name: str):
    df.to_sql(table_name,
              con=conn,
              chunksize=50_000,
              if_exists="append",
              method='multi')
