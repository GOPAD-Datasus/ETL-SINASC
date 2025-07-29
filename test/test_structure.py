import unittest

import pandas as pd
import os


class TestFileStructure (unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_fdr = '../data/processed'
        cls.list = [cls.file_fdr + '/' + f
                    for f in os.listdir(cls.file_fdr)
                    if f.endswith('.gzip')]

        cls.column_order = ['IDADEMAE', 'DTNASCMAE', 
                            'RACACORMAE', 'ESTCIVMAE',
                            'QTDFILVIVO', 'QTDFILMORT',
                            'QTDGESTANT', 'QTDPARTNOR',
                            'QTDPARTCES', 'PARIDADE',
                            'ESCMAE', 'ESCMAE2010',
                            'SERIESCMAE', 'ESCMAEAGR1',
                            'CODMUNNATU', 'CODUFNATU',
                            'NATURALMAE', 'CODMUNRES',
                            'CODOCUPMAE', 'DTULTMENST',
                            'SEMAGESTAC', 'GESTACAO',
                            'GRAVIDEZ', 'CONSPRENAT',
                            'CONSULTAS', 'MESPRENAT',
                            'KOTELCHUCK', 'PARTO',
                            'TPAPRESENT', 'STTRABPART',
                            'STCESPARTO', 'TPROBSON',
                            'TPNASCASSI', 'DTNASC',
                            'HORANASC', 'APGAR1', 'APGAR5',
                            'PESO', 'SEXO', 'RACACOR',
                            'IDANOMAL', 'CODANOMAL',
                            'LOCNASC', 'CODESTAB',
                            'CODMUNNASC', 'IDADEPAI',
                            'TPMETESTIM', 'STDNEPIDEM',
                            'STDNNOVA']


    def test_structure(self):
        for item in self.list:
            df = pd.read_parquet(item)

            self.assertEqual(len(df.columns), 49,
                             f'Num Error: {item}')

            self.assertEqual(df.columns.to_list(),
                             self.column_order,
                             f'Order Error: {item}')


if __name__ == '__main__':
    unittest.main()