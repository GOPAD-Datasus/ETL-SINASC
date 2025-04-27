from typing import Union

import pandas as pd


class YearHandler:
    def __init__ (self, url: str):
        self.url = url
        self.df = pd.DataFrame()

    def parse (self):
        """
        Parse functions help treat mixed type and non-standard
        values in columns
        Ex. parse_sexo in Handler2014, which changes
            'M' to 1, 'F' to 2 and 'I' to 0
        """
        pass

    def remove_cols (self, target: Union[list, str]):
        """
        Removes target columns from dataframe
        param:
            target (list | str): targeted columns to remove
        """
        self.df.drop(target, axis=1, inplace=True)

    def rename_cols (self, target_dict: dict):
        """
        Rename columns
        param:
            target_dict (dict): old name: new name
        """
        self.df.rename(target_dict, axis=1, inplace=True)

    def pipeline (self) -> pd.DataFrame:
        """
        Main pipeline to handle database/file specific
        changes and formatting
        return:
            pd.DataFrame: Standardized dataframe
        """
        return pd.read_csv(self.url)