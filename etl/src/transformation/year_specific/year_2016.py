import pandas as pd

from etl.src.transformation.year_specific.yearHandler import YearHandler


class Handler2016 (YearHandler):
    def pipeline(self):
        sep = ';'

        return pd.read_csv(self.url,
                           sep=sep)