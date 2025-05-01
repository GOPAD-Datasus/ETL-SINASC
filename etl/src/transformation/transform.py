from etl.src.extraction.utils import check_file_exists
from etl.src.transformation.utils import *
from etl.src.transformation.year_specific import apply_year_specific_changes


def transform(params: dict) -> None:
    initial_year = params['starting_year']
    final_year = params['ending_year'] + 1

    pro_folder = '../temp/processed'
    raw_folder = '../temp/raw'

    for year in range(initial_year, final_year):
        input_file = f'{raw_folder}/DN{year}.csv'
        output_file = f'{pro_folder}/DN{year}.parquet.gzip'

        if check_file_exists (output_file):
            continue

        df = apply_year_specific_changes(input_file, year)

        # General changes
        df = remove_cols(df)
        df = optimize_dtypes(df)

        df.to_parquet(output_file,
                      compression='gzip')