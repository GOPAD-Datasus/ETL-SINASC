import importlib
import os.path

import pandas as pd


def get_info(input_files, output_folder, extension):
    output_files = []
    handlers     = []

    for file in input_files:
        leaf = os.path.basename(file)
        split, _ = leaf.split('.')

        handlers.append(split)

        output_files.append(os.path.join(output_folder,
                                         f'{split}{extension}'))

    return output_files, handlers


def handle_transformation(input_path: str,
                          handler: str,
                          output_path: str) -> pd.DataFrame:
    """
    Calls each file's corresponding .py dynamically
    using getattr. Executes the pipeline associated.

    param:
        path (str): Path to the .csv file
        year (int): File's corresponding year
    """
    package = f'etl.transformation.years.{handler}'
    worker = getattr(importlib.import_module(package),
                      handler)

    return worker(input_path).pipeline(output_path)