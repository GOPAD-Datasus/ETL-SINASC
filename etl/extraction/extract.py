import os
from urllib.request import urlretrieve

from etl.extraction.utils import get_urls, make_data_folder
from utils import check_file_exists


def extract(prefix) -> None:
    """
    Main extraction function. Downloads from every url
    inside the files key from input.yaml file.
    """
    extension = '.csv'

    files = get_urls()

    make_data_folder()

    base_dir = os.path.dirname(os.path.abspath(__name__))
    raw_folder = os.path.join(base_dir, 'data', 'raw')

    for year in files.keys():
        source_link = files[year]
        output_file = os.path.join(raw_folder,
                                   f'{prefix}{year}{extension}')

        if not check_file_exists(output_file):
            try:
                urlretrieve(source_link,
                            output_file)
            except Exception as e:
                error_msg = (
                    f"Failed to download {year}: {e}\n"
                    f"Please verify the url in input.yaml"
                )
                print(f'Warning: {error_msg}')