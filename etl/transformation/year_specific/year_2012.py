import pandas as pd

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2012 (YearHandler):
    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '24120198',
                    target] = '24121980'


    def pipeline(self, output_file: str):
        dtype = {'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str,
                 'HORANASC': str}

        self.df = pd.read_csv(self.url, dtype=dtype)

        self.add_cols(['PARIDADE', 'ESCMAEAGR1',
                       'TPNASCASSI', 'KOTELCHUCK',
                       'CODUFNATU'])

        self.handle_na('STDNNOVA')
        self.handle_na('PARIDADE')
        self.handle_na('KOTELCHUCK')

        self.parse_dtnascmae()

        self.remove_cols(['Unnamed: 0', 'CODCART',
                          'NUMREGCART', 'DTREGCART'])

        self.rename_cols({'contador': 'CONTADOR'})

        # RACACORN is the actual RACACOR
        # while RACACOR is the same as RACACORMAE
        # See docs/transformations/RACACOR.ipynb
        # for more details
        self.remove_cols('RACACOR')
        self.rename_cols({'RACACORN': 'RACACOR'})

        super().pipeline(output_file)
