import pandas as pd

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2015 (YearHandler):
    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '13090198',
                    target] = '13091986'


    def pipeline(self):
        dtype = {'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str}
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.parse_dtnascmae()
        self.remove_cols('DTRECORIGA')

        return self.df