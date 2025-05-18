import unittest

import pandas as pd


class TestStructure (unittest.TestCase):
    def test_structure(self):
        start = 2012
        end = 2024

        path = '../temp/processed'
        list_files = [f'{path}/DN{i}.parquet.gzip'
                      for i in range(start, end)]

        order = ['IDADEMAE', 'DTNASCMAE', 'RACACORMAE',
                 'ESTCIVMAE', 'QTDFILVIVO', 'QTDFILMORT',
                 'QTDGESTANT', 'QTDPARTNOR', 'QTDPARTCES',
                 'PARIDADE', 'ESCMAE', 'ESCMAE2010',
                 'SERIESCMAE', 'ESCMAEAGR1', 'CODMUNNATU',
                 'CODUFNATU', 'NATURALMAE', 'CODMUNRES',
                 'CODOCUPMAE', 'DTULTMENST', 'SEMAGESTAC',
                 'GESTACAO', 'GRAVIDEZ', 'CONSPRENAT',
                 'CONSULTAS', 'MESPRENAT', 'KOTELCHUCK',
                 'PARTO', 'TPAPRESENT', 'STTRABPART',
                 'STCESPARTO', 'TPROBSON', 'TPNASCASSI',
                 'DTNASC', 'HORANASC', 'APGAR1', 'APGAR5',
                 'PESO', 'SEXO', 'RACACOR', 'IDANOMAL',
                 'CODANOMAL', 'LOCNASC', 'CODESTAB',
                 'CODMUNNASC', 'IDADEPAI', 'TPMETESTIM',
                 'STDNEPIDEM', 'STDNNOVA']

        for item in list_files:
            df = pd.read_parquet(item)

            self.assertEqual(len(df.columns),
                             len(order))

            self.assertEqual(df.columns.to_list(),
                             order)

if __name__ == '__main__':
    unittest.main()