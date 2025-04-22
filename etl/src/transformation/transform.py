import pandas as pd
import numpy as np
import json


def drop (df: pd.DataFrame, type_: str) -> pd.DataFrame:
    if type_ == 'system':
        list_ = ['contador', 'ORIGEM', 'NUMEROLOTE',
                 'VERSAOSIST', 'DTRECEBIM', 'DIFDATA',
                 'DTCADASTRO']
    elif type_ == 'registry':
        list_ = ['CODCART', 'NUMREGCART', 'DTREGCART']

    # TODO: check if columns exist inside df

    return df.drop(list_, axis=1)

def merge_mother_education (df: pd.DataFrame) -> pd.DataFrame:
    # TODO: improve and move to utils.py
    df.loc[df['SERIESCMAE'] == 0, 'SERIESCMAE'] = np.nan
    df.loc[df['SERIESCMAE'] == 9, 'SERIESCMAE'] = np.nan

    conditions = [
        df['ESCMAE2010'] == 0,
        (df['ESCMAE2010'] == 1) & (pd.isna(df['SERIESCMAE'])),  # FI
        (df['ESCMAE2010'] == 1) & (df['SERIESCMAE'] >= 1),
        (df['ESCMAE2010'] == 2) & (pd.isna(df['SERIESCMAE'])),  # FII
        (df['ESCMAE2010'] == 2) & (df['SERIESCMAE'] >= 5),
        (df['ESCMAE2010'] == 3) & (pd.isna(df['SERIESCMAE'])),  # M
        (df['ESCMAE2010'] == 3) & (df['SERIESCMAE'] >= 1),
        df['ESCMAE2010'] == 4,
        df['ESCMAE2010'] == 5
    ]

    choices = [
        0,
        1,
        df['SERIESCMAE'],
        5,
        df['SERIESCMAE'],
        9,
        9 + (df['SERIESCMAE'] - 1),
        12,
        13
    ]

    df['EDUCATION_LEVEL'] = np.select(conditions, choices, default=np.nan)

    conditions = [
        (pd.isna(df['ESCMAE'])) & (pd.isna(df['EDUCATION_LEVEL'])),
        (pd.notna(df['EDUCATION_LEVEL'])),
        (df['ESCMAE'] == 1) & (pd.isna(df['EDUCATION_LEVEL'])),
        (df['ESCMAE'] == 2) & (pd.isna(df['EDUCATION_LEVEL'])),
        (df['ESCMAE'] == 3) & (pd.isna(df['EDUCATION_LEVEL'])),
        (df['ESCMAE'] == 4) & (pd.isna(df['EDUCATION_LEVEL'])),
        (df['ESCMAE'] == 5) & (pd.isna(df['EDUCATION_LEVEL']))
    ]

    choices = [
        np.nan,
        df['EDUCATION_LEVEL'],
        0,
        1,
        4,
        8,
        12
    ]

    df.drop(['ESCMAE', 'ESCMAE2010', 'SERIESCMAE'],
            axis=1, inplace=True)

    df['EDUCATION_LEVEL'] = np.select(conditions, choices, default=np.nan)

    return df

def merge_newborn_race (df: pd.DataFrame) -> pd.DataFrame:
    conditions = [
        (pd.isna(df['RACACORN'])) & (pd.notna(df['RACACOR'])),
        (pd.notna(df['RACACORN'])) & (pd.isna(df['RACACOR'])),
        df['RACACORN'] == df['RACACOR'],
        df['RACACOR'] != df['RACACORN']
    ]

    choices = [
        df['RACACOR'],
        df['RACACORN'],
        df['RACACOR'],
        df['RACACOR']
    ]

    df['RACACOR'] = np.select(conditions, choices, default=np.nan)
    df.drop(['RACACORN'], axis=1, inplace=True)

    return df

def datetime_dtype (df: pd.DataFrame) -> pd.DataFrame :
    # TODO: add other columns too
    list_ = ['DTULTMENST', 'DTNASCMAE', 'DTNASC']

    for item in list_:
        df[item] = df[item].fillna(0).astype(int).apply(lambda x: str(x) if len(str(x)) == 8 else '0' + str(x))
        df[item] = pd.to_datetime(df[item], format='%d%m%Y', errors='coerce')

    return df

def optimize_dtypes (df: pd.DataFrame) -> pd.DataFrame:
    with open('../parameters/transform_dtype.json') as json_file:
        dict_ = json.load(json_file)

    for i in df.columns:
        try:
            df[i] = df[i].astype(dict_[i])
        except KeyError:
            pass

def transform(params: dict) -> None:
    df = pd.read_csv('DN2012.csv') #TODO: change URL
    pd.options.mode.copy_on_write = True

    # TODO: remove unnamed: 0 and other unnamed columns
    # df.drop(['Unnamed: 0'], axis=1, inplace=True)

    # TODO: implement gopad structure
    if params['drop_system'] == 'yes':
        df = drop(df, 'system')

    if params['drop_registry_office'] == 'yes':
        df = drop(df, 'registry')

    if params['merge_mother_education'] == 'yes':
        df = merge_mother_education(df)

    if params['merge_newborn_race'] == 'yes':
        df = merge_newborn_race(df)