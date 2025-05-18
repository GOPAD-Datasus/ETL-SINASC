import pandas as pd

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2017 (YearHandler):
    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '20010199',
                    target] = '20011990'


    def pipeline(self):
        dtype = {'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str,
                 'HORANASC': str}
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.parse_dtnascmae()

        self.remove_cols(['DTRECORIGA', 'DTDECLARAC',
                          'TPFUNCRESP', 'TPDOCRESP'])

        return self.df