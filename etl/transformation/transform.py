from etl.extraction.utils import check_file_exists
from etl.transformation.year_specific import handle_year


def transform(params: dict) -> None:
    initial_year = params['starting_year']
    final_year = params['ending_year'] + 1

    pro_folder = 'temp/processed'
    raw_folder = 'temp/raw'

    for year in range(initial_year, final_year):
        input_file = f'{raw_folder}/DN{year}.csv'
        output_file = f'{pro_folder}/DN{year}.parquet.gzip'

        if check_file_exists (output_file):
            continue

        handle_year(input_file, year, output_file)