from etl.src.extraction.extract import extract
from etl.src.transformation.transform import transform
import json

if __name__ == '__main__':
    with open('../parameters/params.json') as json_file:
        params = json.load(json_file)

    extract(params)
    transform(params)