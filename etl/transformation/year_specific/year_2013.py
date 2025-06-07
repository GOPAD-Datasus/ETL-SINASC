import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2013 (YearHandler):
    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '27091193',
                    target] = '27091993'


    def parse_idade_pai (self):
        target = 'IDADEPAI'

        self.df.loc[self.df[target] == '5D', target] = 50
        self.df[target] = self.df[target].astype(np.float32)


    def pipeline (self) -> pd.DataFrame:
        dtype = {'IDADEPAI': str,
                 'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str,
                 'HORANASC': str}

        self.df = pd.read_csv(self.url,
                              dtype=dtype)

        self.add_cols(['TPROBSON', 'PARIDADE',
                       'CONSPRENAT', 'KOTELCHUCK'])

        self.handle_na('PARIDADE')
        self.handle_na('KOTELCHUCK')

        self.parse_dtnascmae()
        self.parse_idade_pai()

        self.remove_cols(['Unnamed: 0', 'DTRECORIG',
                          'CODCART', 'NUMREGCART',
                          'DTREGCART', 'CODMUNCART'])

        self.rename_cols({'contador': 'CONTADOR'})

        # RACACORN is the same as RACACOR, so it
        # can be safely deleted.
        # See docs/transformations/RACACOR.ipynb
        # for more details
        self.remove_cols('RACACORN')

        return self.df