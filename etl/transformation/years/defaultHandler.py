import numpy as np

from transformation.handlers import Handler, TimeHandler


class DefaultHandler(Handler, TimeHandler):
    def __init__(self, url: str):
        super().__init__(url)
        self.format = '%d%m%Y'

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

    def pipeline(self, output_file: str) -> None:
        """
        Main pipeline to handle database/file specific
        changes and formatting
        return:
            pd.DataFrame: Standardized dataframe
        """

        self.modify_idanomal()

        self.modify_dates(self.df['DTNASC'])
        self.modify_dates(self.df['DTNASCMAE'])
        self.modify_dates(self.df['DTULTMENST'])

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
