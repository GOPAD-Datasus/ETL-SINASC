import os
from pathlib import Path
from typing import List


def check_file_exists (path: str) -> bool:
    """
    Returns True if a file exists. False if otherwise.

    param:
        path (str): Path leading to the file
    return:
        bool: State of the file
    """
    return os.path.isfile(path)


def load_data (folder_url: str, endswith: str) -> List[str]:
    return [os.path.join(folder_url, f)
            for f in os.listdir(folder_url)
            if f.endswith(endswith)]


def get_base_dir() -> Path:
    """
    Return the project root directory
    """
    try:
        return Path(__file__).resolve().parent.parent
    except NameError:
        return Path.cwd()


def get_raw_folder () -> str:
    return os.path.join(get_base_dir(), 'data', 'raw')


def get_processed_folder () -> str:
    return os.path.join(get_base_dir(), 'data', 'processed')