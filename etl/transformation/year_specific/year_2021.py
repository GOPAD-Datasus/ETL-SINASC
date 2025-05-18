import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2021 (YearHandler):
    def parse_cod_uf_natu (self):
        target = 'CODUFNATU'
        self.df.loc[self.df[target] == 'nu',
                    target] = np.nan
        self.df[target] = self.df[target].astype(np.float32)


    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '22011199',
                    target] = '22011991'


    def pipeline(self):
        dtype = {'CODUFNATU': str,
                 'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str,
                 'HORANASC': str}
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.parse_cod_uf_natu()
        self.parse_dtnascmae()

        self.remove_cols(['DTRECORIGA', 'DTDECLARAC',
                          'TPFUNCRESP', 'TPDOCRESP'])

        return self.df