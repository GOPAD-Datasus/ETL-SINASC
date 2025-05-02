import pandas as pd

from etl.src.transformation.year_specific.yearHandler import YearHandler


class Handler2017 (YearHandler):
    def pipeline(self):
        sep = ';'

        self.df = pd.read_csv(self.url,
                              sep=sep)

        self.remove_cols('DTRECORIGA')

        return self.df