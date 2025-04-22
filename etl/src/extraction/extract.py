import urllib.error
from urllib.request import urlretrieve
import os
import json


def load_urls () -> dict:
    with open('../parameters/extract_urls.json') as json_file:
        return json.load(json_file)

def make_temp_folder () -> str:
    temp_folder = '../temp'
    if not os.path.exists(temp_folder):
        os.mkdir(temp_folder)
    return temp_folder

def check_file_exists (path: str) -> bool:
    return os.path.isfile(path)

def get_from_opendatasus (params: dict):
    inicial_year = params['starting_year']
    final_year = params['ending_year']

    dict_ = load_urls()
    temp_folder = make_temp_folder()

    for i in range(inicial_year, final_year):
        link = dict_[f'{i}']
        output_file = f'{temp_folder}/DN{i}.csv'

        if not check_file_exists(output_file):
            try:
                urlretrieve(link,
                            output_file)
            except urllib.error.URLError:
                error_ = (f'Url corresponding to year {i}'
                          f' is wrong, try checking it on'
                          f' extract_urls.json')
                raise RuntimeError(error_)
                # TODO: improve error handling:
                # to allow continuous download of
                # other files

def extract(params: dict) -> None:
    if params['source'] == 'opendatasus':
        get_from_opendatasus(params)