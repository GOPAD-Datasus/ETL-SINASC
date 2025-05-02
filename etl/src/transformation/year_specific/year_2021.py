import pandas as pd
import numpy as np

from etl.src.transformation.year_specific.yearHandler import YearHandler


class Handler2021 (YearHandler):
    def parse_cod_uf_natu (self):
        target = 'CODUFNATU'
        self.df.loc[self.df[target] == 'nu',
                    target] = np.nan
        self.df[target] = self.df[target].astype(np.float32)

    def pipeline(self):
        dtype = {'CODUFNATU': str}
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.parse_cod_uf_natu()
        self.remove_cols('DTRECORIGA')

        return self.df