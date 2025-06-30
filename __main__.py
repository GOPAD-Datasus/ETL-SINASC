from etl.extraction import extract
from etl.transformation import transform
import json


if __name__ == '__main__':
    with open('parameters/params.json') as json_file:
        params = json.load(json_file)

    extract(params)
    transform(params)
    # load()