import pandas as pd
import numpy as np


def _parse_dates(x):
    if type(x) == str:
        area = x[4:6]

        if area == '09':
            return x[:4] + '19' + x[6:]
        elif area == '00':
            return x[:4] + '20' + x[6:]
        else:
            return x
    else:
        return x


class HandlerGeneral:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df


    def remove_cols(self):
        """
        Removes columns with info about the system used to
        collect the data at the hospitals. Those columns
        are not very useful for analysis and are in every
        file, qualifying as a general transformation.
        All years have those columns.
        Columns are 'CONTADOR', 'ORIGEM', 'NUMEROLOTE',
        'VERSAOSIST', 'DTRECEBIM', 'DIFDATA', 'DTCADASTRO'
        and 'CODPAISRES'
        """
        list_ = ['CONTADOR', 'ORIGEM', 'NUMEROLOTE',
                 'VERSAOSIST', 'DTRECEBIM', 'DIFDATA',
                 'DTCADASTRO', 'CODPAISRES']

        self.df.drop(list_, axis=1, inplace=True)


    def remove_ignored_values(self):
        """
        Removes the ignored category from columns.
        An ignored value is a value that wasn't collected,
        was out of the column's limit or was chosen to not
        be collected.
        """
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
            'TPMETESTIM': 9,
            'CONSPRENAT': 99,
            'TPAPRESENT': 9,
            'STTRABPART': 9,
            'STCESPARTO': 9
        }

        self.df.replace(values, np.nan, inplace=True)


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


    def pipeline(self, output_file: str):
        self.modify_idanomal()
        self.modify_dates('DTNASC')
        self.modify_dates('DTNASCMAE')
        self.modify_dates('DTULTMENST')

        self.remove_cols()
        self.remove_ignored_values()
        self.optimize_dtypes()

        self.df.to_parquet(output_file,
                           compression='gzip')