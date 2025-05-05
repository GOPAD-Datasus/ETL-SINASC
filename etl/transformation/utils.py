import pandas as pd
import numpy as np


def remove_cols (df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes columns with info about the system used to
    collect the data at the hospitals. Those columns
    are not very useful for analysis and are in every
    file, qualifying as a general transformation.
    All years have those columns.
    Columns are 'CONTADOR', 'ORIGEM', 'NUMEROLOTE',
    'VERSAOSIST', 'DTRECEBIM', 'DIFDATA' and 'DTCADASTRO'
    param:
        df (pd.DataFrame): DataFrame to remove columns
    return:
        pd.DataFrame: DataFrame without columns
    """
    list_ = ['CONTADOR', 'ORIGEM', 'NUMEROLOTE',
             'VERSAOSIST', 'DTRECEBIM', 'DIFDATA',
             'DTCADASTRO']

    return df.drop(list_, axis=1)


def remove_ignored_values (df: pd.DataFrame) -> pd.DataFrame:
    values = {
        'APGAR5': 99,
        'CONSULTAS': 9,
        'GESTACAO': 9,
        'MESPRENAT': 99,
        'IDADEMAE': 99,
        'ESTCIVMAE': 9,
        'ESCMAE2010': 9,
        'PARTO': 9,
        'QTDFILVIVO': 99,
        'QTDFILMORT': 99,
        'TPROBSON': 11
    }

    df.replace(values, np.nan, inplace=True)

    return df


def optimize_dtypes (df: pd.DataFrame) -> pd.DataFrame:
    """
    CSV files, used as extension for all SINASC files,
    can't hold info. about dtypes, and pandas defaults
    to 64 format. This function aims to reduce the
    memory used
    param:
        df (pd.DataFrame): DataFrame before with many
                           int64 and float64 as dtypes
    return:
        pd.DataFrame: DataFrame after, with better
                      memory usage
    """
    for i in df.columns:
        if df[i].dtype == np.int64:
            df[i] = df[i].astype(np.int32)
        elif df[i].dtype == np.float64:
            df[i] = df[i].astype(np.float32)
    return df


def modify_idanomal (series: pd.Series) -> np.array:
    conditions = [
        (series == 1),
        (series == 2)
    ]

    choices = [
        1, # Sim
        0  # Nao
    ]

    return np.select(conditions,
                     choices,
                     default=0)
