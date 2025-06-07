from typing import Union

import pandas as pd
import numpy as np


class YearHandler:
    def __init__ (self, url: str):
        self.url = url
        self.df = pd.DataFrame()

    def add_cols (self, target: Union[list, str]):
        """
        Add the cols to the dataframe and fill them with
        np.nan.

        param:
            target (list | str):
        """
        if type(target) == str:
            self.df[target] = np.nan
        if type(target) == list:
            for i in target:
                self.df[i] = np.nan


    def handle_na (self, target: str):
        self.df.fillna({target: 0}, inplace=True)
        self.df[target] = self.df[target].astype(int)


    def parse (self):
        """
        Parse functions (defined by 'parse_') help treat
        mixed type and non-standard values in columns.
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