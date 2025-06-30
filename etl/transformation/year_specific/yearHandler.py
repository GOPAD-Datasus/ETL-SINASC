from typing import Union

import pandas as pd
import numpy as np

from transformation.year_specific.utils import _parse_dates


class YearHandler:
    def __init__ (self, url: str):
        self.url = url
        self.df = pd.DataFrame()

    def add_cols (self, target: Union[list, str]):
        """
        Add the cols to the dataframe and fill them with
        np.nan.

        param:
            target (list | str):
        """
        if type(target) == str:
            self.df[target] = np.nan
        if type(target) == list:
            for i in target:
                self.df[i] = np.nan


    def handle_na (self, target: str):
        self.df.fillna({target: 0}, inplace=True)
        self.df[target] = self.df[target].astype(int)


    def parse (self):
        """
        Parse functions (defined by 'parse_') help treat
        mixed type and non-standard values in columns.
        Ex. parse_sexo in Handler2014, which changes
            'M' to 1, 'F' to 2 and 'I' to 0
        """
        pass


    def remove_cols (self, target: Union[list, str]):
        """
        Removes target columns from dataframe
        param:
            target (list | str): targeted columns to remove
        """
        self.df.drop(target, axis=1, inplace=True)


    def remove_ignored_values(self, values: dict):
        """
        Removes the ignored category from columns.
        An ignored value is a value that wasn't collected,
        was out of the column's limit or was chosen to not
        be collected.
        """
        self.df.replace(values, np.nan, inplace=True)


    def rename_cols (self, target_dict: dict):
        """
        Rename columns
        param:
            target_dict (dict): old name: new name
        """
        self.df.rename(target_dict, axis=1, inplace=True)


    def modify_idanomal(self):
        """
        IDANOMAL has a confusing structure, where 1
        represents 'Yes' and 2 'No'. The boolean format
        (0: No, 1: Yes) seems better for this scenario
        This also treats null values, filling it with 0
        """
        target = 'IDANOMAL'

        conditions = [(self.df[target] == 1),
                      (self.df[target] == 2)]
        choices = [1, 0]

        self.df[target] = np.select(conditions,
                          choices,
                          default=0)


    def modify_dates(self, target):
        """
        Standardizes columns related to date by
        converting to datetime. Minimizing the need
        to choose str dtype for them and increasing
        date integrity
        """
        self.df[target] = pd.to_datetime((self.df[target]
                                          .apply(_parse_dates)),
                                         format='%d%m%Y')


    def optimize_dtypes(self):
        """
        CSV files, used as extension for all SINASC files,
        can't hold info. about dtypes, and pandas defaults
        to 64 format. This function aims to reduce the
        memory used by converting to 32 instead
        """
        for i in self.df.columns:
            if self.df[i].dtype == np.int64:
                self.df[i] = self.df[i].astype(np.int32)
            elif self.df[i].dtype == np.float64:
                self.df[i] = self.df[i].astype(np.float32)


    def organize_columns(self, order: list):
        self.df = self.df[order]


    def pipeline (self, output_file: str) -> None:
        """
        Main pipeline to handle database/file specific
        changes and formatting
        return:
            pd.DataFrame: Standardized dataframe
        """

        self.modify_idanomal()

        self.modify_dates('DTNASC')
        self.modify_dates('DTNASCMAE')
        self.modify_dates('DTULTMENST')

        list_ = ['CONTADOR', 'ORIGEM', 'NUMEROLOTE',
                 'VERSAOSIST', 'DTRECEBIM', 'DIFDATA',
                 'DTCADASTRO', 'CODPAISRES']
        self.remove_cols(list_)

        values = {
            'CODESTAB':   8888888,
            'LOCNASC':    9,
            'ESCMAE':     9,
            'GRAVIDEZ':   9,
            'APGAR1':     99,
            'APGAR5':     99,
            'CONSULTAS':  9,
            'GESTACAO':   9,
            'MESPRENAT':  99,
            'IDADEMAE':   99,
            'ESTCIVMAE':  9,
            'ESCMAE2010': 9,
            'PARTO':      9,
            'QTDFILVIVO': 99,
            'QTDFILMORT': 99,
            'TPROBSON':   11,
            'PESO':       9999,
            'CODMUNNATU': 999999,
            'QTDGESTANT': 99,
            'QTDPARTNOR': 99,
            'QTDPARTCES': 99,
            'IDADEPAI':   99,
            'TPMETESTIM': [8, 9],
            'CONSPRENAT': 99,
            'TPAPRESENT': 9,
            'STTRABPART': 9,
            'STCESPARTO': 9,
            'TPNASCASSI': 9
        }
        self.remove_ignored_values(values)

        self.optimize_dtypes()

        order = ['IDADEMAE', 'DTNASCMAE', 'RACACORMAE',
                 'ESTCIVMAE', 'QTDFILVIVO', 'QTDFILMORT',
                 'QTDGESTANT', 'QTDPARTNOR', 'QTDPARTCES',
                 'PARIDADE', 'ESCMAE', 'ESCMAE2010',
                 'SERIESCMAE', 'ESCMAEAGR1', 'CODMUNNATU',
                 'CODUFNATU', 'NATURALMAE', 'CODMUNRES',
                 'CODOCUPMAE', 'DTULTMENST', 'SEMAGESTAC',
                 'GESTACAO', 'GRAVIDEZ', 'CONSPRENAT',
                 'CONSULTAS', 'MESPRENAT', 'KOTELCHUCK',
                 'PARTO', 'TPAPRESENT', 'STTRABPART',
                 'STCESPARTO', 'TPROBSON', 'TPNASCASSI',
                 'DTNASC', 'HORANASC', 'APGAR1', 'APGAR5',
                 'PESO', 'SEXO', 'RACACOR', 'IDANOMAL',
                 'CODANOMAL', 'LOCNASC', 'CODESTAB',
                 'CODMUNNASC', 'IDADEPAI', 'TPMETESTIM',
                 'STDNEPIDEM', 'STDNNOVA']
        self.organize_columns(order)

        self.df.to_parquet(output_file, compression='gzip')