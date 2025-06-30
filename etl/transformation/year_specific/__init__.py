import importlib
import pandas as pd


def handle_year (path: str, year: int, output_path: str) -> pd.DataFrame:
    """
    Calls each year's corresponding .py dynamically
    using getattr, simplifying the inclusion of more
    years

    param:
        path (str): Path to the .csv file
        year (int): File's corresponding year
    """
    package = f'etl.transformation.year_specific.year_{year}'
    handler = getattr(importlib.import_module(package),
                      f'Handler{year}')

    return handler(path).pipeline(output_path)
