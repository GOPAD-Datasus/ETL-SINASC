import urllib.error
from urllib.request import urlretrieve

from etl.src.extraction.utils import *


def extract(params: dict) -> None:
    """
    Main extraction function. Downloads every file inside
    the interval [starting_year, ending_year].
    The source urls can be found inside get_urls() from
    utils

    param:
        params: dict of parameters
    """
    initial_year = params['starting_year']
    final_year = params['ending_year'] + 1

    dict_ = get_urls()

    make_temp_folder()
    raw_folder = '../temp/raw'

    for i in range(initial_year, final_year):
        source_link = dict_[i]
        output_file = f'{raw_folder}/DN{i}.csv'

        if not check_file_exists(output_file):
            try:
                urlretrieve(source_link,
                            output_file)
            except urllib.error.URLError:
                error_ = (f'Url corresponding to year {i}'
                          f' is wrong, try changing on'
                          f' extraction/utils.py inside'
                          f' get_urls()')
                raise RuntimeWarning(error_)