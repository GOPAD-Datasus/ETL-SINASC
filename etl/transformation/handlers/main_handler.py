from typing import Union

import pandas as pd
import numpy as np


class Handler:
    def __init__(self, url: str):
        self.url = url
        self.df = pd.DataFrame()

    def add_cols(self, target: Union[list, str]):
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

    def handle_na(self, target: str):
        self.df.fillna({target: 0}, inplace=True)
        self.df[target] = self.df[target].astype(int)

    def remove_cols(self, target: Union[list, str]):
        """
        Removes target columns from dataframe
        param:
            target (list | str): targeted columns to remove
        """
        self.df.drop(target, axis=1, inplace=True)

    def remove_ignored_values(self, values: dict):
        """
        Removes the ignored category from columns.
        An ignored value is a value that wasn't collected,
        was out of the column's limit or was chosen to not
        be collected.
        """
        self.df.replace(values, np.nan, inplace=True)

    def rename_cols(self, target_dict: dict):
        """
        Rename columns
        param:
            target_dict (dict): old name: new name
        """
        self.df.rename(target_dict, axis=1, inplace=True)

    def optimize_dtypes(self):
        """
        CSV files, used as extension for all SINASC files,
        can't hold info. about dtypes, and pandas defaults
        to 64 format. This function aims to reduce the
        memory used by converting to 32 instead
        """
        for i in self.df.columns:
            if self.df[i].dtype == np.int64:
                self.df[i] = self.df[i].astype(np.int32)
            elif self.df[i].dtype == np.float64:
                self.df[i] = self.df[i].astype(np.float32)

    def organize_columns(self, order: list):
        self.df = self.df[order]
