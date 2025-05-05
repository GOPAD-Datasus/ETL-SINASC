import os


def get_urls () -> dict:
    """
    Returns a dictionary with links corresponding to each year
    In case the location of a single or multiple databases
    change, altering it here can solve the problem.

    returns:
        dict: Pair year (int) and link (str) for all desirable
              years
    """
    return {
        2012: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2012.csv',
        2013: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2013.csv',
        2014: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2014.csv',
        2015: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2015.csv',
        2016: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2016.csv',
        2017: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2017.csv',
        2018: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2018.csv',
        2019: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2019.csv',
        2020: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2020.csv',
        2021: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2021.csv',
        2022: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2022.csv',
        2023: 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2023.csv'
    }


def make_temp_folder () -> None:
    """
    Creates the temporary file structure to hold raw and
    processed files. The structure is defined below:

    + temp: root folder
      - raw: raw data extracted from source
      - processed: processed data
    """
    temp_folder = '../temp'
    if not os.path.exists(temp_folder):
        os.mkdir(temp_folder)

    raw_folder = temp_folder + '/raw'
    if not os.path.exists(raw_folder):
        os.mkdir(raw_folder)

    processed_folder = temp_folder + '/processed'
    if not os.path.exists(processed_folder):
        os.mkdir(processed_folder)


def check_file_exists (path: str) -> bool:
    """
    Returns True if a file exists. False if otherwise.

    param:
        path (str): Path leading to the file
    return:
        bool: State of the file
    """
    return os.path.isfile(path)