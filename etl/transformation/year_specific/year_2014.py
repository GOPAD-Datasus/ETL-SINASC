import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2014 (YearHandler):
    def parse_hora_nasc (self):
        target = 'HORANASC'
        table = str.maketrans('', '', ',/-?>')
        self.df[target] = (self.df[target]
                           .str.translate(table))

    def parse_sexo (self):
        target = 'SEXO'
        table = str.maketrans('MFI', '120')
        self.df[target] = (self.df[target]
                           .str.translate(table)
                           .astype(np.int32))


    def pipeline(self):
        dtype = {'HORANASC': str,
                 'SEXO': str,
                 'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str}
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.handle_na('STDNNOVA')

        self.parse_hora_nasc()
        self.parse_sexo()

        self.remove_cols(['DTRECORIGA', 'DTDECLARAC',
                          'TPFUNCRESP', 'TPDOCRESP'])

        return self.df