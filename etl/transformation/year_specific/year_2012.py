import pandas as pd

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2012 (YearHandler):
    def pipeline(self) -> pd.DataFrame:
        self.df = pd.read_csv(self.url)

        self.add_cols(['PARIDADE', 'TPFUNCRESP', 'ESCMAEAGR1',
                       'DTDECLARAC', 'TPDOCRESP', 'TPNASCASSI',
                       'KOTELCHUCK', 'CODUFNATU'])

        self.remove_cols('Unnamed: 0')
        self.remove_cols(['CODCART', 'NUMREGCART', 'DTREGCART'])
        self.rename_cols({'contador': 'CONTADOR'})

        # RACACORN is the actual RACACOR
        # while RACACOR is the same as RACACORMAE
        # See docs/transformations/RACACOR.ipynb
        # for more details
        self.remove_cols('RACACOR')
        self.rename_cols({'RACACORN': 'RACACOR'})

        return self.df

