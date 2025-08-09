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


    # # TODO: Remove necessity
    # initial_year = params['starting_year']
    # final_year = params['ending_year'] + 1
    #
    # extension = '.parquet.gzip'
    #
    #
    #
    # # raw_files = load_data(raw_folder, '.csv')
    #
    # # pro_files = []
    # # for file in raw_files:
    # #     file = str(os.path.basename(file))[:-4]
    # #     pro_files.append(os.path.join(pro_folder, f'{file}{extension}'))
    # #
    # # print(pro_files)
    #
    # for year in range(initial_year, final_year):
    #     input_file = f'{raw_folder}/DN{year}.csv'
    #     output_file = f'{pro_folder}/DN{year}.parquet.gzip'
    #
    #     if check_file_exists (output_file):
    #         continue
    #
    #     handle_year(input_file, year, output_file)