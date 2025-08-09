import pandas as pd
import numpy as np

from transformation.years import DefaultHandler


class DN2023 (DefaultHandler):
    def parse_cod_uf_natu (self):
        target = 'CODUFNATU'
        self.df.loc[self.df[target] == 'nu',
                    target] = np.nan
        self.df[target] = self.df[target].astype(np.float32)


    def parse_dtnascmae (self):
        target = 'DTNASCMAE'

        self.df.loc[self.df[target] == '04080200',
                    target] = '04082000'


    def pipeline(self, output_file: str):
        dtype = {'CODUFNATU': str,
                 'DTNASC': str,
                 'DTNASCMAE': str,
                 'DTULTMENST': str,
                 'HORANASC': str}
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.parse_cod_uf_natu()
        self.parse_dtnascmae()

        self.remove_cols(['OPORT_DN', 'DTRECORIGA',
                          'DTDECLARAC', 'TPFUNCRESP',
                          'TPDOCRESP'])
        self.rename_cols({'contador': 'CONTADOR'})

        super().pipeline(output_file)