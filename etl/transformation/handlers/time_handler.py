from datetime import datetime

import pandas as pd


class TimeHandler:
    def __init__(self, format_):
        self.format = format_

    def correct_year(self, date_str: str):
        if type(date_str) == str:
            try:
                date = datetime.strptime(date_str, self.format)
            except ValueError:
                return date_str

            if date.year >= 900 or date.year <= 999:
                correct_year = date.year + 1000
            elif date.year >= 0 or date.year <= 30:
                correct_year = date.year + 2000
            else:
                return date_str

            try:
                return (datetime(year=correct_year,
                                month=date.month,
                                day=date.day)
                        .strftime(format=self.format))
            except ValueError:
                return date_str

        else:
            return date_str

    def modify_dates(self, series):
        """
        Standardizes columns related to date by
        converting to datetime. Minimizing the need
        to choose str dtype for them and increasing
        date integrity
        """
        return pd.to_datetime(series.apply(self.correct_year),
                              format=self.format,
                              errors='coerce')