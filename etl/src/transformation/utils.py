import pandas as pd
import numpy as np


def remove_cols (df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes columns with info about the system used to
    collect the data at the hospitals. Those columns
    are not very useful for analysis.
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