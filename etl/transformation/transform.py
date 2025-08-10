import os.path

from transformation.utils import get_info, handle_transformation
from utils import check_file_exists, load_data, get_base_dir, get_raw_folder, \
    get_processed_folder


def transform() -> None:
    raw_folder = get_raw_folder()
    pro_folder = get_processed_folder()

    extension = '.parquet.gzip'

    raw_files = load_data(raw_folder, '.csv')
    pro_files, handlers = \
        get_info(raw_files, pro_folder, extension)

    for in_file, out_file, handler in \
            zip(raw_files, pro_files, handlers):
        if not check_file_exists(out_file):
            handle_transformation(in_file, handler, out_file)
