import pandas as pd

from transformation.years import DefaultHandler


class DN2016 (DefaultHandler):
    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '30070199',
                    target] = '30071997'
        self.df.loc[self.df[target] == '21050199',
                    target] = '21051996'


    def pipeline(self, output_file: str):
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

        super().pipeline(output_file)